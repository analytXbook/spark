package org.apache.spark.scheduler.flare

import java.io.NotSerializableException
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.util.{Clock, SystemClock, Utils}
import org.apache.spark._

import scala.collection.mutable.{HashMap, HashSet, ListBuffer, MultiMap, Set}
import scala.collection.JavaConverters._
import scala.util.Random
import scala.util.control.NonFatal


private[spark] class FlareReservationManager(
    scheduler: FlareScheduler,
    val taskSet: TaskSet,
    maxTaskFailures: Int,
    clock: Clock = new SystemClock()) extends Logging{
  
  val conf = scheduler.sc.conf
  val env = SparkEnv.get
  val ser = env.closureSerializer.newInstance()
  
  val probeRatio = taskSet.properties.getProperty("spark.flare.probeRatio", "2.0").toDouble
  val parallelismLimit = Option(taskSet.properties.getProperty("spark.flare.parallelismLimit")).map(_.toInt)
  val limitExecutorParallelism = Option(taskSet.properties.getProperty("spark.flare.limitExecutorParallelism")).map(_.toInt)

  val EXCEPTION_PRINT_INTERVAL =
    conf.getLong("spark.logging.exceptionPrintInterval", 10000)
  
  val maxResultSize = Utils.getMaxResultSize(conf)
  
  val tasks = taskSet.tasks
  val numTasks = tasks.length
    
  val launchedConstrainedTasks = new HashMap[String, Int]
  
  val unlaunchedConstrainedTasks = new HashMap[String, Set[Int]] with MultiMap[String, Int]
  val unlaunchedUnconstrainedTasks = new ListBuffer[Int]
  
  val pendingReservations = new HashMap[String, Int]
  
  val runningTasks = new HashSet[Long]

  def runningTaskCount: Int = runningTasks.size

  val taskInfos = new ConcurrentHashMap[Long, TaskInfo].asScala

  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)
  val successful = new Array[Boolean](numTasks)

  var tasksSuccessful = 0

  val recentExceptions = HashMap[String, (Int, Long)]()

  val epoch = scheduler.mapOutputTracker.getEpoch
  logDebug("Epoch for " + taskSet + ": " + epoch)
  for (t <- tasks) {
    t.epoch = epoch
  }

  var isZombie = false

  def addRunningTask(taskId: Long) {
    runningTasks.add(taskId)
  }

  def removeRunningTask(taskId: Long) {
    runningTasks.remove(taskId)
  }

  def executorFromHost(host: String): Option[String] = {
    scheduler.executorsByHost.get(host).flatMap(executors =>
      if (executors.isEmpty) None
      else Some(executors(Random.nextInt(executors.length))))
  }

  def randomExecutor: String = {
    scheduler.executors(Random.nextInt(scheduler.executors.length))
  }

  def getReservations: Map[String, Int] = {
    val reservations = new ListBuffer[String]
    for (index <- 0 until numTasks) {
      val task = tasks(index)
      if (task.preferredLocations.isEmpty)
        unlaunchedUnconstrainedTasks += index
      else {
        val preferredExecutors = task.preferredLocations.flatMap {
          case ExecutorCacheTaskLocation(host, executorId) => Some(executorId)
          case taskLocation => executorFromHost(taskLocation.host)
        }

        if (preferredExecutors.isEmpty) {
          logDebug(s"Could not match any preferred locations for task $index in stage ${task.stageId} to executor, treating as unconstrained")
          unlaunchedUnconstrainedTasks += index
        } else {
          for (targetExecutor <- Random.shuffle(preferredExecutors.take(probeRatio.round.toInt))) {
            unlaunchedConstrainedTasks.addBinding(targetExecutor, index)
            reservations += targetExecutor
          }
        }
      }
    }

    val remainingReservations = Math.ceil(unlaunchedUnconstrainedTasks.size * probeRatio).toInt
    for (i <- 0 until remainingReservations) {
      reservations += randomExecutor
    }

    val reservationCounts = reservations.groupBy(identity).mapValues(_.size)
    pendingReservations ++= reservationCounts

    reservationCounts
  }

  def getMatchedLocality(task: Task[_], executorId: String, host: String): TaskLocality.Value = {
    var highestLocality = TaskLocality.ANY
    for (location <- task.preferredLocations) {
      location match {
        case ExecutorCacheTaskLocation(_, execId) =>
          if (executorId == execId)
            highestLocality = TaskLocality.PROCESS_LOCAL
        case location =>
          if (location.host == host && highestLocality != TaskLocality.PROCESS_LOCAL)
            highestLocality = TaskLocality.NODE_LOCAL
      }
    }
    highestLocality
  }

  def abort(message: String, exception: Option[Throwable] = None) = {
    scheduler.dagScheduler.taskSetFailed(taskSet, message, exception)
    attemptFinishReservation()
  }

  def executorLost(executorId: String, host: String, reason: ExecutorLossReason)  = {

  }

  def canFetchMoreResults(size: Long): Boolean = true

  def handleTaskGettingResult(taskId: Long): Unit = {
    val info = taskInfos(taskId)
    info.markGettingResult()
    scheduler.dagScheduler.taskGettingResult(info)
  }

  def attemptFinishReservation() = {
    if (isZombie && runningTasks == 0) {
      scheduler.taskSetFinished(this)
    }
  }

  def handleSuccessfulTask(taskId: Long, result: DirectTaskResult[_]) = {
    val taskInfo = taskInfos(taskId)
    val index = taskInfo.index
    taskInfo.markSuccessful()
    removeRunningTask(taskId)

    scheduler.dagScheduler.taskEnded(
      tasks(index), Success, result.value(), result.accumUpdates, taskInfo, result.metrics)

    if (!successful(index)) {
      tasksSuccessful += 1
      logInfo("Finished task %s in stage %s (TID %d) in %d ms on %s (%d/%d)".format(
        taskInfo.id, taskSet.id, taskInfo.taskId, taskInfo.duration, taskInfo.host, tasksSuccessful, numTasks))

      successful(index) = true
      if (tasksSuccessful == numTasks) {
        isZombie = true
      }
    } else {
      logInfo("Ignoring task-finished event for " + taskInfo.id + " in stage " + taskSet.id +
        " because task " + index + " has already completed successfully")
    }


    attemptFinishReservation()
  }


  def handleFailedTask(taskId: Long, state: TaskState, reason: TaskEndReason): Unit = {
    val taskInfo = taskInfos(taskId)
    val index = taskInfo.index
    if (taskInfo.failed) {
      return
    }
    removeRunningTask(taskId)
    taskInfo.markFailed()

    var taskMetrics: TaskMetrics = null

    val failureReason = s"Lost task ${taskInfo.id} in stage ${taskSet.id} (TID $taskId, ${taskInfo.host}): " +
      reason.asInstanceOf[TaskFailedReason].toErrorString

    val failureException: Option[Throwable] = reason match {
      case fetchFailed: FetchFailed =>
        logWarning(failureReason)
        if (!successful(index)) {
          successful(index) = true
          tasksSuccessful += 1
        }
        // Not adding to failed executors for FetchFailed.
        isZombie = true
        None
      case ef: ExceptionFailure =>
        taskMetrics = ef.metrics.orNull
        if (ef.className == classOf[NotSerializableException].getName) {
          // If the task result wasn't serializable, there's no point in trying to re-execute it.
          logError("Task %s in stage %s (TID %d) had a not serializable result: %s; not retrying"
            .format(taskInfo.id, taskSet.id, taskId, ef.description))
          abort("Task %s in stage %s (TID %d) had a not serializable result: %s".format(
            taskInfo.id, taskSet.id, taskId, ef.description))
          return
        }
        val key = ef.description
        val now = clock.getTimeMillis()
        val (printFull, dupCount) = {
          if (recentExceptions.contains(key)) {
            val (dupCount, printTime) = recentExceptions(key)
            if (now - printTime > EXCEPTION_PRINT_INTERVAL) {
              recentExceptions(key) = (0, now)
              (true, 0)
            } else {
              recentExceptions(key) = (dupCount + 1, printTime)
              (false, dupCount + 1)
            }
          } else {
            recentExceptions(key) = (0, now)
            (true, 0)
          }
        }
        if (printFull) {
          logWarning(failureReason)
        } else {
          logInfo(
            s"Lost task ${taskInfo.id} in stage ${taskSet.id} (TID $taskId) on executor ${taskInfo.host}: " +
              s"${ef.className} (${ef.description}) [duplicate $dupCount]")
        }
        ef.exception

      case e: ExecutorLostFailure if !e.exitCausedByApp =>
        logInfo(s"Task $taskId failed because while it was being computed, its executor" +
          "exited for a reason unrelated to the task. Not counting this failure towards the " +
          "maximum number of failures for the task.")
        None

      case e: TaskFailedReason =>  // TaskResultLost, TaskKilled, and others
        logWarning(failureReason)
        None

      case e: TaskEndReason =>
        logError("Unknown TaskEndReason: " + e)
        None
    }



    scheduler.dagScheduler.taskEnded(tasks(index), reason, null, null, taskInfo, taskMetrics)
  }

  def isMaxParallelism: Boolean = parallelismLimit.fold(false)(runningTaskCount >= _)
   
  def getTask(executorId: String, host: String): Option[TaskDescription] = {
    pendingReservations(executorId) -= 1

    val currentTime = clock.getTimeMillis()
    val selectedTask: Option[Int] = {
      if (unlaunchedConstrainedTasks.contains(executorId)) {
        val index = unlaunchedConstrainedTasks(executorId).head
        unlaunchedConstrainedTasks.removeBinding(executorId, index)
        Some(index)
      }
      else if (!unlaunchedUnconstrainedTasks.isEmpty) {
        Some(unlaunchedUnconstrainedTasks.remove(0))
      }
      else {
        None
      }
    }
    
    selectedTask.map { index =>
      val task = tasks(index)
      val taskId = scheduler.newTaskId
      
      val taskLocality = getMatchedLocality(task, executorId, host)
      
      val attemptNumber = taskAttempts(index).size
      val info = new TaskInfo(taskId, index, attemptNumber, currentTime, executorId, host, taskLocality, false)
      
      taskInfos(taskId) = info
      taskAttempts(index) = info :: taskAttempts(index)
      
      val serializedTask: ByteBuffer = try {
        Task.serializeWithDependencies(task, scheduler.sc.addedFiles, scheduler.sc.addedJars, ser)
      } catch {
        case NonFatal(e) => {
          val msg = s"Failed to serialize task $taskId, not attempting to retry it."
          logError(msg, e)
          abort(s"$msg Exception during serialization: $e")
          throw new TaskNotSerializableException(e)
        }
      }
      
      val taskName = s"task ${info.id} in stage ${taskSet.id}"
      logInfo(s"Starting $taskName (TID $taskId, $host, $executorId, partition ${task.partitionId}," +
            s"$taskLocality, ${serializedTask.limit} bytes)")
      
      scheduler.dagScheduler.taskStarted(task, info)
      new TaskDescription(taskId, attemptNumber, executorId, taskName, index, serializedTask)
    }
  }  
}