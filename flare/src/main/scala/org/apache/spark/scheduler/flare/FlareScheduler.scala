package org.apache.spark.scheduler.flare

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.TaskState.TaskState
import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.EncodedId

import scala.collection.mutable.{ArrayBuffer, HashMap}


private[spark] class FlareScheduler(val sc: SparkContext) extends TaskScheduler with Logging {
  val conf = sc.conf

  var dagScheduler: DAGScheduler = _
  var backend: FlareSchedulerBackend = _
  
  val cpusPerTask = conf.getInt("spark.task.cpus", 1)
  val maxTaskFailures = conf.getInt("spark.task.maxFailures", 4)
  
  val reservationsByStageIdAndAttempt = new HashMap[Int, HashMap[Int, FlareReservationManager]]
  
  val nextTaskId = new AtomicLong(0)
  
  def newTaskId = EncodedId.encodeIfEnabled(nextTaskId.getAndIncrement)
  
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
      tid: Long,
      taskState: TaskState,
      reason: TaskEndReason): Unit = synchronized {
    reservationManager.handleFailedTask(tid, taskState, reason)
  }
   
  override def submitTasks(taskSet: TaskSet): Unit = {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    
    this.synchronized {
      val manager = createReservationManager(taskSet)
      val stageReservations =
        reservationsByStageIdAndAttempt.getOrElseUpdate(taskSet.stageId, new HashMap[Int, FlareReservationManager])
      stageReservations(taskSet.stageAttemptId) = manager
      
      val reservations = manager.getReservations
      
      backend.placeReservations(taskSet.stageId, taskSet.stageAttemptId, reservations)
    }
  }
  
  def isMaxParallelism(stageId: Int, stageAttemptId: Int): Boolean =
    reservationsByStageIdAndAttempt(stageId)(stageAttemptId).isMaxParallelism
  
  def redeemReservation(stageId: Int, stageAttemptId: Int, executorId: String, host: String): Option[TaskDescription] = {
    reservationsByStageIdAndAttempt.get(stageId).flatMap(_.get(stageAttemptId)) match {
      case Some(manager) => {
        val taskOpt = manager.getTask(executorId, host)
        taskOpt.foreach { task => 
          taskIdToReservationManager(task.taskId) = manager
          taskIdToExecutorId(task.taskId) = executorId
        }
        taskOpt
      }
      case None => {
        logError(s"Could not find reservation manager for $stageId, $stageAttemptId")
        None
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
    reservationsByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, manager) =>
        manager.runningTasksSet.foreach { taskId => 
          val executorId = taskIdToExecutorId(taskId)
          backend.killTask(taskId, executorId, interruptThread)
        }
        
        manager.abort("Stage %s cancelled".format(stageId))
        logInfo("Stage %d was cancelled".format(stageId))
      }
    }
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