package org.apache.spark.scheduler.flare

import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.TaskState.TaskState
import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util._
import org.apache.spark.internal.Logging

import scala.collection.mutable


private[spark] class FlareScheduler(val sc: SparkContext) extends TaskScheduler with Logging {
  val conf = sc.conf

  val CHECK_LOCALITY_WAIT_INTERVAL = conf.getTimeAsMs("spark.locality.wait.interval", "100ms")

  var dagScheduler: DAGScheduler = _
  var backend: FlareSchedulerBackend = _
  
  val cpusPerTask = conf.getInt("spark.task.cpus", 1)
  val maxTaskFailures = conf.getInt("spark.task.maxFailures", 4)

  val managersByStageIdAndAttempt = new mutable.HashMap[Int, mutable.HashMap[Int, FlareReservationManager]]
  
  val taskIdGenerator = IdGenerator.long("task")
  
  def newTaskId = taskIdGenerator.next
  
  val mapOutputTracker = SparkEnv.get.mapOutputTracker
 
  var taskResultGetter = new FlareTaskResultGetter(sc.env, this)

  val taskIdToReservationManager = new mutable.HashMap[Long, FlareReservationManager]
  val taskIdToExecutor = new mutable.HashMap[Long, String]

  val executors = new mutable.ArrayBuffer[String]
  val executorsByHost = new mutable.HashMap[String, mutable.ArrayBuffer[String]]
  val executorToHost = new mutable.HashMap[String, String]

  private val executorIdToRunningTaskIds = new mutable.HashMap[String, mutable.HashSet[Long]]

  private val localityWaitScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("task-locality-wait-scheduler")

  override def setDAGScheduler(dagScheduler: DAGScheduler): Unit = {
    this.dagScheduler = dagScheduler
  }

  def initialize(backend: FlareSchedulerBackend) = {
    this.backend = backend
  }

  override val schedulingMode: SchedulingMode = SchedulingMode.FIFO

  override val rootPool: Pool = new Pool("", schedulingMode, 0, 0)


  private def addExecutor(executorId: String, host: String) = {
    val hostExecutors = executorsByHost.getOrElseUpdate(host, new mutable.ArrayBuffer)
    hostExecutors += executorId
    executorToHost(executorId) = host
    executors += executorId
  }
  override def start() = {
    backend.start()

    localityWaitScheduler.scheduleAtFixedRate(new Runnable {
      override def run() = Utils.tryOrStopSparkContext(sc) {
        checkLocalityWaitReservations()
      }
    }, CHECK_LOCALITY_WAIT_INTERVAL, CHECK_LOCALITY_WAIT_INTERVAL, TimeUnit.MILLISECONDS)

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
        managersByStageIdAndAttempt.getOrElseUpdate(taskSet.stageId, new mutable.HashMap[Int, FlareReservationManager])
      stageManagers(taskSet.stageAttemptId) = manager

      val reservations = manager.getReservations
      val groups = manager.reservationGroups

      backend.placeReservations(taskSet.stageId, taskSet.stageAttemptId, reservations, groups)
    }
  }

  override def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean = {
    logInfo(s"Killing task $taskId: $reason")
    val executorId = taskIdToExecutor.get(taskId)
    if (executorId.isDefined) {
      backend.killTask(taskId, executorId.get, interruptThread, reason)
      true
    } else {
      logWarning(s"Could not kill task $taskId because no task with that ID was found.")
      false
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
          taskIdToExecutor(task.taskId) = executorId
          executorIdToRunningTaskIds.getOrElseUpdate(executorId, mutable.HashSet[Long]()).add(task.taskId)
        }
        taskOpt
      }
    }
  }

  def statusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer) {
    synchronized {
      try {
        taskIdToReservationManager.get(taskId) match {
          case Some(reservationManager) => {
            if (TaskState.isFinished(state)) {
              taskIdToReservationManager.remove(taskId)
              taskIdToExecutor.remove(taskId)
            }

            if (TaskState.isFinished(state)) {
              cleanUpTaskState(taskId)
              reservationManager.removeRunningTask(taskId)
              if (state == TaskState.FINISHED) {
                taskResultGetter.enqueueSuccessfulTask(reservationManager, taskId, serializedData)
              } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
                taskResultGetter.enqueueFailedTask(reservationManager, taskId, state, serializedData)
              }
            }
          }
          case None => {
            logError(
              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
                "likely the result of receiving duplicate task finished status updates) or its " +
                "executor has been marked as failed.")
                .format(state, taskId))
          }
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
  }


  override def cancelTasks(stageId: Int, interruptThread: Boolean) = synchronized {
    logInfo("Cancelling stage " + stageId)
    managersByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, manager) =>
        manager.runningTasks.foreach { taskId =>
          val executorId = taskIdToExecutor(taskId)
          backend.killTask(taskId, executorId, interruptThread, "stage cancelled")
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
    managersByStageIdAndAttempt.get(taskSet.stageId).foreach { attemptManagers =>
      attemptManagers.remove(taskSet.stageAttemptId)
      if (attemptManagers.isEmpty) {
        managersByStageIdAndAttempt.remove(taskSet.stageId)
      }
    }
  }

  def checkLocalityWaitReservations() = synchronized {
    managersByStageIdAndAttempt.foreach {
      case (stageId, attempts) => {
        attempts.foreach {
          case (stageAttemptId, manager) => {
            val reservations = manager.checkLocalityTimeout()
            if (!reservations.isEmpty) {
              backend.placeReservations(stageId, stageAttemptId, reservations, manager.reservationGroups)
            }
          }
        }
      }
    }
  }

  override def defaultParallelism(): Int = backend.defaultParallelism()

  override def executorHeartbeatReceived(execId: String,
                                         accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
                                         blockManagerId: BlockManagerId): Boolean = {
    // (taskId, stageId, stageAttemptId, accumUpdates)
    val accumUpdatesWithTaskIds: Array[(Long, Int, Int, Seq[AccumulableInfo])] = synchronized {
      accumUpdates.flatMap { case (id, updates) =>
        // We should call `acc.value` here as we are at driver side now.  However, the RPC framework
        // optimizes local message delivery so that messages do not need to de serialized and
        // deserialized.  This brings trouble to the accumulator framework, which depends on
        // serialization to set the `atDriverSide` flag.  Here we call `acc.localValue` instead to
        // be more robust about this issue.
        val accInfos = updates.map(acc => acc.toInfo(Some(acc.value), None))
        taskIdToReservationManager.get(id).map { reservationManager =>
          (id, reservationManager.taskSet.stageId, reservationManager.taskSet.stageAttemptId, accInfos)
        }
      }
    }
    dagScheduler.executorHeartbeatReceived(execId, accumUpdatesWithTaskIds, blockManagerId)
  }

  private def cleanUpTaskState(taskId: Long): Unit = {
    taskIdToReservationManager.remove(taskId)
    taskIdToExecutor.remove(taskId).foreach { executorId =>
      executorIdToRunningTaskIds.get(executorId).foreach(_.remove(taskId))
    }
  }

  private def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
    executorIdToRunningTaskIds.remove(executorId).foreach { taskIds =>
      logDebug("Cleaning up scheduler state for tasks " +
        s"${taskIds.mkString("[", ",", "]")} on failed executor $executorId")

      taskIds.foreach(cleanUpTaskState)

      val host = executorToHost(executorId)
      val hostExecutors = executorsByHost.getOrElse(host, new mutable.ArrayBuffer)
      hostExecutors -= executorId

      if (reason != LossReasonPending) {
        executorToHost -= executorId
        managersByStageIdAndAttempt.foreach {
          case (stageId, attempts) => {
            attempts.foreach {
              case (stageAttemptId, manager) => {
                val replacementReservations = manager.executorLost(executorId, host, reason)
                if (!replacementReservations.isEmpty) {
                  backend.placeReservations(stageId, stageAttemptId, replacementReservations, manager.reservationGroups)
                }
              }
            }
          }
        }
      }
    }
  }

  override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    var failedExecutor: Option[String] = None

    synchronized {
      if (executorIdToRunningTaskIds.contains(executorId)) {
        val hostPort = executorToHost(executorId)
        logExecutorLoss(executorId, hostPort, reason)
        removeExecutor(executorId, reason)
        failedExecutor = Some(executorId)
      } else {
        executorToHost.get(executorId) match {
          case Some(hostPort) => {
            logExecutorLoss(executorId, hostPort, reason)
            removeExecutor(executorId, reason)
          }
          case None => {
            logError(s"Lost an executor $executorId (already removed): $reason")
          }
        }
      }
    }
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get, reason)
    }
  }

  private def logExecutorLoss(
    executorId: String,
    hostPort: String,
    reason: ExecutorLossReason): Unit = reason match {
    case LossReasonPending =>
      logDebug(s"Executor $executorId on $hostPort lost, but reason not yet known.")
    case ExecutorKilled =>
      logInfo(s"Executor $executorId on $hostPort killed by driver.")
    case _ =>
      logError(s"Lost executor $executorId on $hostPort: $reason")
  }

  def executorRegistered(executorId: String, host: String): Unit = {
    addExecutor(executorId, host)
  }
  
  override def stop(): Unit = {
    localityWaitScheduler.shutdown()
    if (backend != null) {
      backend.stop()
    }
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }
  }
   
}