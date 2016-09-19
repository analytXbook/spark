package org.apache.spark.scheduler.flare

import java.io.NotSerializableException
import java.nio.ByteBuffer
import java.util.Properties

import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.util.{Clock, SystemClock, Utils}
import org.apache.spark._

import scala.collection.mutable.{HashMap, HashSet, ArrayBuffer, MultiMap, Set, ListBuffer}
import scala.collection.mutable
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

  val unlaunchedConstrainedTasks = new HashMap[String, Set[Int]] with mutable.SynchronizedMap[String, Set[Int]] with MultiMap[String, Int]
  val unlaunchedUnconstrainedTasks = new ArrayBuffer[Int] with mutable.SynchronizedBuffer[Int]

  val pendingConstrainedTaskReservations = new HashMap[Int, Set[String]] with mutable.SynchronizedMap[Int, Set[String]] with MultiMap[Int, String]
  val pendingReservations = (new HashMap[String, Int] with mutable.SynchronizedMap[String, Int]).withDefaultValue(0)

  val runningTasks = new HashSet[Long] with mutable.SynchronizedSet[Long]

  def runningTaskCount: Int = runningTasks.size

  val taskInfos = new HashMap[Long, TaskInfo] with mutable.SynchronizedMap[Long, TaskInfo]

  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)
  val successful = new Array[Boolean](numTasks)

  var tasksSuccessful = 0

  val numFailures = new Array[Int](numTasks)

  val recentExceptions = HashMap[String, (Int, Long)]()

  val failedExecutors = new HashMap[Int, HashMap[String, Long]]()

  val epoch = scheduler.mapOutputTracker.getEpoch
  logDebug("Epoch for " + taskSet + ": " + epoch)
  for (t <- tasks) {
    t.epoch = epoch
  }

  var isZombie = false


  val reservationGroups = {
    def getGroupDescription(properties: Properties, index: Int): Option[FlarePoolDescription] = {
      val prefix = s"spark.flare.pool[$index]"
      Option(properties.getProperty(s"$prefix.name")).map { name =>
        val maxShare = Option(properties.getProperty(s"$prefix.maxShare")).map(_.toInt)
        val minShare = Option(properties.getProperty(s"$prefix.minShare")).map(_.toInt)
        val weight = Option(properties.getProperty(s"$prefix.weight")).map(_.toInt)
        FlarePoolDescription(name, minShare, maxShare, weight)
      }
    }

    var groups = Seq.empty[FlarePoolDescription]

    var groupIndex = 0
    var nextGroup = getGroupDescription(taskSet.properties, groupIndex)

    while (nextGroup.isDefined) {
      groups = groups :+ nextGroup.get
      groupIndex += 1
      nextGroup = getGroupDescription(taskSet.properties, groupIndex)
    }

    groups
  }

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
            pendingConstrainedTaskReservations.addBinding(index, targetExecutor)
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

  def replacementReservations(taskId: Long): Map[String, Int] = {
    val index = taskInfos(taskId).index
    val task = tasks(index)

    val triedExecutors = failedExecutors.get(index).map(_.keySet.toSeq).getOrElse(Seq.empty)

    val executor = if (task.preferredLocations.isEmpty) {
      val untriedExecutors = scheduler.executors.diff(triedExecutors)
      unlaunchedUnconstrainedTasks += index

      if (untriedExecutors.isEmpty) {
        logDebug(s"All executors have been tried for failed task $taskId, selecting a random executor")
        randomExecutor
      } else {
        untriedExecutors(Random.nextInt(untriedExecutors.length))
      }
    } else {
      val pendingExecutors = pendingConstrainedTaskReservations.get(index).map(_.toSeq).getOrElse(List.empty)

      val preferredExecutors = task.preferredLocations.flatMap {
        case ExecutorCacheTaskLocation(host, executorId) => List(executorId)
        case taskLocation => scheduler.executorsByHost.get(taskLocation.host).getOrElse(List.empty)
      }.diff(pendingExecutors).diff(triedExecutors)

      if (preferredExecutors.isEmpty) {
        logDebug(s"No additional executors could be found matching placement constraints for failed task $taskId, allowing task to run unconstrained")
        unlaunchedUnconstrainedTasks += index
        randomExecutor
      } else {

        val targetExecutor = preferredExecutors(Random.nextInt(preferredExecutors.length))
        unlaunchedConstrainedTasks.addBinding(targetExecutor, index)
        targetExecutor
      }
    }
    pendingReservations(executor) += 1
    Map(executor -> 1)
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
    isZombie = true
    maybeFinishTaskSet()
  }


  def canFetchMoreResults(size: Long): Boolean = true

  def handleTaskGettingResult(taskId: Long): Unit = {
    val info = taskInfos(taskId)
    info.markGettingResult()
    scheduler.dagScheduler.taskGettingResult(info)
  }

  def maybeFinishTaskSet() = {
    if (isZombie && runningTasks.isEmpty) {
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

    maybeFinishTaskSet()
  }


  def handleFailedTask(taskId: Long, state: TaskState, reason: TaskEndReason): Map[String, Int] = {
    val taskInfo = taskInfos(taskId)
    val index = taskInfo.index
    if (taskInfo.failed) {
      return Map.empty
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
          return Map.empty
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

    failedExecutors.getOrElseUpdate(index, new HashMap[String, Long]()).put(taskInfo.executorId, clock.getTimeMillis())

    scheduler.dagScheduler.taskEnded(tasks(index), reason, null, null, taskInfo, taskMetrics)

    if (!isZombie && state != TaskState.KILLED
      && reason.isInstanceOf[TaskFailedReason]
      && reason.asInstanceOf[TaskFailedReason].countTowardsTaskFailures) {
      numFailures(index) += 1
      if (numFailures(index) >= maxTaskFailures) {
        val failureReason = s"Task $index in stage ${taskSet.id} failed $maxTaskFailures times"
        logError(s"$failureReason; aborting job")
        abort(s"$failureReason, most recent failure: \n Driver Stacktrace:", failureException)
        return Map.empty
      }
    }

    maybeFinishTaskSet()
    replacementReservations(taskId)
  }

  def isMaxParallelism: Boolean = parallelismLimit.fold(false)(runningTaskCount >= _)

  def getTask(executorId: String, host: String): Option[TaskDescription] = {
    pendingReservations(executorId) -= 1

    val currentTime = clock.getTimeMillis()
    val selectedTask: Option[Int] = {
      if (unlaunchedConstrainedTasks.contains(executorId)) {
        val index = unlaunchedConstrainedTasks(executorId).head
        pendingConstrainedTaskReservations.removeBinding(index, executorId)
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
          abort(s"$msg Exception during serialization", Some(e))
          throw new TaskNotSerializableException(e)
        }
      }
      
      val taskName = s"task ${info.id} in stage ${taskSet.id}"
      logDebug(s"Starting $taskName (TID $taskId, $host, $executorId, partition ${task.partitionId}," +
            s"$taskLocality, ${serializedTask.limit} bytes)")
      
      scheduler.dagScheduler.taskStarted(task, info)
      new TaskDescription(taskId, attemptNumber, executorId, taskName, index, serializedTask)
    }
  }  
}