package org.apache.spark.scheduler.flare

import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.TaskState.TaskState
import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{EncodedId, LongIdGenerator}

import scala.collection.mutable.{ArrayBuffer, HashMap}


private[spark] class FlareScheduler(val sc: SparkContext) extends TaskScheduler with Logging {
  val conf = sc.conf

  var dagScheduler: DAGScheduler = _
  var backend: FlareSchedulerBackend = _
  
  val cpusPerTask = conf.getInt("spark.task.cpus", 1)
  val maxTaskFailures = conf.getInt("spark.task.maxFailures", 4)
  
  val managersByStageIdAndAttempt = new HashMap[Int, HashMap[Int, FlareReservationManager]]
  
  val taskIdGenerator = LongIdGenerator(sc)
  
  def newTaskId = taskIdGenerator.next
  
  val mapOutputTracker = SparkEnv.get.mapOutputTracker
 
  var taskResultGetter = new FlareTaskResultGetter(sc.env, this)
  
  val taskIdToReservationManager = new HashMap[Long, FlareReservationManager]
  val taskIdToExecutorId = new HashMap[Long, String]

  val executors = new ArrayBuffer[String]
  val executorsByHost = new HashMap[String, ArrayBuffer[String]]
  
  override def setDAGScheduler(dagScheduler: DAGScheduler): Unit = {
    this.dagScheduler = dagScheduler
  }
  
  def initialize(backend: FlareSchedulerBackend) = {
    this.backend = backend
  }
  
  override val schedulingMode: SchedulingMode = SchedulingMode.FIFO

  override val rootPool: Pool = new Pool("", schedulingMode, 0, 0)


  private def addExecutor(executorId: String, host: String) = {
    val hostExecutors = executorsByHost.getOrElseUpdate(host, new ArrayBuffer[String])
    hostExecutors += executorId
    executors += executorId
  }
  override def start() = {
    backend.start()
    
    backend.executors.foreach {
      case (executorId, executor) => {
        addExecutor(executorId, executor.executorHost)
      }
    }
  }
  
  override def postStartHook(): Unit = {
    if (backend.isReady) {
      return
    }

    logInfo("Waiting for backend to be ready")
    while (!backend.isReady){
      synchronized {
        this.wait(100)
      }
    }
  }
  
  override def applicationId(): String = backend.applicationId()
  
  override def applicationAttemptId(): Option[String] = backend.applicationAttemptId()
  
  private def createReservationManager(taskSet: TaskSet) = {
    new FlareReservationManager(this, taskSet, maxTaskFailures)
  }
  
  def handleTaskGettingResult(reservationManager: FlareReservationManager, taskId: Long): Unit = synchronized {
    reservationManager.handleTaskGettingResult(taskId)
  }
  
  def handleSuccessfulTask(
      reservationManager: FlareReservationManager,
      taskId: Long,
      result: DirectTaskResult[_]): Unit = synchronized {
    reservationManager.handleSuccessfulTask(taskId, result)
  }
  
  def handleFailedTask(
      reservationManager: FlareReservationManager,
      taskId: Long,
      taskState: TaskState,
      reason: TaskEndReason): Unit = synchronized {
    val replacementReservations = reservationManager.handleFailedTask(taskId, taskState, reason)
    if (!replacementReservations.isEmpty) {
      logDebug(s"Adding additional reservation to executors: ${replacementReservations.mkString(",")} for failed task $taskId")
      val taskSet = reservationManager.taskSet
      val reservationGroups = reservationManager.reservationGroups
      backend.placeReservations(taskSet.stageId, taskSet.stageAttemptId, replacementReservations, reservationGroups)
    }
  }

  override def submitTasks(taskSet: TaskSet): Unit = {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    
    this.synchronized {
      val manager = createReservationManager(taskSet)
      val stageManagers =
        managersByStageIdAndAttempt.getOrElseUpdate(taskSet.stageId, new HashMap[Int, FlareReservationManager])
      stageManagers(taskSet.stageAttemptId) = manager
      
      val reservations = manager.getReservations
      val groups = manager.reservationGroups

      backend.placeReservations(taskSet.stageId, taskSet.stageAttemptId, reservations, groups)
    }
  }
  
  def isMaxParallelism(stageId: Int, stageAttemptId: Int): Boolean =
    managersByStageIdAndAttempt(stageId)(stageAttemptId).isMaxParallelism
  
  def redeemReservation(stageId: Int, stageAttemptId: Int, executorId: String, host: String): Option[TaskDescription] = synchronized {
    managersByStageIdAndAttempt.get(stageId).flatMap(_.get(stageAttemptId)).flatMap {
      manager => {
        val taskOpt = manager.getTask(executorId, host)
        taskOpt.foreach { task => 
          taskIdToReservationManager(task.taskId) = manager
          taskIdToExecutorId(task.taskId) = executorId
        }
        taskOpt
      }
    }
  }
  
  def statusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer) {
    val failedExecutor: Option[String] = None
    synchronized {
      try {
        taskIdToReservationManager.get(taskId) match {
          case Some(reservationManager) => {
            if (TaskState.isFinished(state)) {
              taskIdToReservationManager.remove(taskId)
              taskIdToExecutorId.remove(taskId)
            }
            
            if (state == TaskState.FINISHED) {
              reservationManager.removeRunningTask(taskId)
              taskResultGetter.enqueueSuccessfulTask(reservationManager, taskId, serializedData)
            } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
              reservationManager.removeRunningTask(taskId)
              taskResultGetter.enqueueFailedTask(reservationManager, taskId, state, serializedData)
            }
          }
          case None => {
            logError(
              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
                "likely the result of receiving duplicate task finished status updates)")
                .format(state, taskId))
          }
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get)
    }
  }

  
  override def cancelTasks(stageId: Int, interruptThread: Boolean) = synchronized {
    logInfo("Cancelling stage " + stageId)
    managersByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, manager) =>
        manager.runningTasks.foreach { taskId =>
          val executorId = taskIdToExecutorId(taskId)
          backend.killTask(taskId, executorId, interruptThread)
        }
        
        manager.abort("Stage %s cancelled".format(stageId))
        logInfo("Stage %d was cancelled".format(stageId))
      }
    }
  }

  def taskSetFinished(reservationManager: FlareReservationManager) = synchronized {
    val executors = reservationManager.pendingReservations.keySet.toSeq
    val taskSet = reservationManager.taskSet

    logDebug(s"Finishing TaskSet: ${taskSet.id}, pending executors: ${executors.mkString(",")}")

    backend.cancelReservations(taskSet.stageId, taskSet.stageAttemptId, executors)
    managersByStageIdAndAttempt.get(taskSet.stageId).flatMap(_.remove(taskSet.stageAttemptId))
  }
  
  override def defaultParallelism(): Int = backend.defaultParallelism()
  
  override def executorHeartbeatReceived(
      executorId: String, 
      taskMetrics: Array[(Long, TaskMetrics)],
      blockManagerId: BlockManagerId): Boolean = {
    
    val metricsWithStageIds: Array[(Long, Int, Int, TaskMetrics)] = synchronized {
      taskMetrics.flatMap { case (id, metrics) =>
        taskIdToReservationManager.get(id).map { reservationManager =>
          (id, reservationManager.taskSet.stageId, reservationManager.taskSet.stageAttemptId, metrics)
        }
      }
    }
    
    dagScheduler.executorHeartbeatReceived(executorId, metricsWithStageIds, blockManagerId)
  }
  
  override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    
  }

  def executorRegistered(executorId: String, host: String): Unit = {
    addExecutor(executorId, host)
  }
  
  override def stop(): Unit = {
    if (backend != null) {
      backend.stop()
    }
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }
  }
   
}