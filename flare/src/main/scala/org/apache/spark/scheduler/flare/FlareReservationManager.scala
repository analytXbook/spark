package org.apache.spark.scheduler.flare

import java.io.NotSerializableException
import java.nio.ByteBuffer
import java.util.Properties

import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.util.{AccumulatorV2, Clock, SystemClock, Utils}
import org.apache.spark._
import org.apache.spark.internal.Logging

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

  val currentLocalityWaitLevel = new mutable.HashMap[Int, (TaskLocality.Value, Long)]()
  val localityLostTasks = new mutable.HashSet[Int] with mutable.SynchronizedSet[Int]

  val unlaunchedConstrainedTasks = new mutable.HashMap[String, mutable.Set[Int]] with mutable.SynchronizedMap[String, mutable.Set[Int]] with mutable.MultiMap[String, Int]
  val unlaunchedUnconstrainedTasks = new mutable.ArrayBuffer[Int] with mutable.SynchronizedBuffer[Int]

  val pendingConstrainedTaskReservations = new mutable.HashMap[Int, mutable.Set[String]] with mutable.SynchronizedMap[Int, mutable.Set[String]] with mutable.MultiMap[Int, String]
  val pendingReservations = (new mutable.HashMap[String, Int] with mutable.SynchronizedMap[String, Int]).withDefaultValue(0)
  val pendingUnconstrainedReservations = new mutable.HashMap[String, Int].withDefaultValue(0)

  val runningTasks = new mutable.HashSet[Long] with mutable.SynchronizedSet[Long]

  def runningTaskCount: Int = runningTasks.size

  val taskInfos = new mutable.HashMap[Long, TaskInfo] with mutable.SynchronizedMap[Long, TaskInfo]

  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)
  val successful = new Array[Boolean](numTasks)

  var tasksSuccessful = 0

  val numFailures = new Array[Int](numTasks)

  val recentExceptions = mutable.HashMap[String, (Int, Long)]()

  val failedExecutors = new mutable.HashMap[Int, mutable.HashMap[String, Long]]()

  var totalResultSize = 0L
  var calculatedTasks = 0

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
    val reservations = new mutable.ListBuffer[String]
    for (index <- 0 until numTasks) {
      val task = tasks(index)

      logDebug(s"Preferred locations for task $index in stage ${task.stageId},${task.stageAttemptId}: ${task.preferredLocations.mkString(",")}")
      logDebug(s"Current Executors: ${scheduler.executorsByHost}")

      if (task.preferredLocations.isEmpty) {
        unlaunchedUnconstrainedTasks += index
      } else {
        var highestLocality = TaskLocality.ANY
        val preferredExecutors = task.preferredLocations.flatMap {
          case ExecutorCacheTaskLocation(host, executorId) => {
            highestLocality = TaskLocality.PROCESS_LOCAL
            Some(executorId)
          }
          case taskLocation => {
            if (highestLocality != TaskLocality.PROCESS_LOCAL) {
              highestLocality = TaskLocality.NODE_LOCAL
            }
            executorFromHost(taskLocation.host)
          }
        }

        if (preferredExecutors.isEmpty) {
          logDebug(s"Could not match any preferred locations for task $index in stage ${task.stageId} to executor, treating as unconstrained")
          unlaunchedUnconstrainedTasks += index
        } else {
          currentLocalityWaitLevel(index) = (highestLocality, clock.getTimeMillis())

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
      val executor = randomExecutor
      reservations += executor
      pendingUnconstrainedReservations(executor) += 1
    }

    val reservationCounts = reservations.groupBy(identity).mapValues(_.size)
    pendingReservations ++= reservationCounts

    reservationCounts
  }


  private def replacementReservations(taskIndex: Int): Map[String, Int] = {
    val task = tasks(taskIndex)

    val triedExecutors = failedExecutors.get(taskIndex).map(_.keySet).getOrElse(Set.empty[String])

    val executor = if (task.preferredLocations.isEmpty) {
      val untriedExecutors = scheduler.executors.filterNot(executor => triedExecutors.contains(executor))
      unlaunchedUnconstrainedTasks += taskIndex

      val targetExecutor = if (untriedExecutors.isEmpty) {
        logDebug(s"All executors have been tried for failed task $taskIndex, selecting a random executor")
        randomExecutor
      } else {
        untriedExecutors(Random.nextInt(untriedExecutors.length))
      }
      pendingUnconstrainedReservations(targetExecutor) += 1
      targetExecutor
    } else {
      val preferredExecutors = task.preferredLocations.flatMap {
        case ExecutorCacheTaskLocation(host, executorId) =>
          if (scheduler.executors.contains(executorId)) List((executorId, host)) else List.empty
        case taskLocation =>
          scheduler.executorsByHost.get(taskLocation.host).getOrElse(List.empty).map(executorId => (executorId, taskLocation.host))
      }.filterNot { case (executor, _) => triedExecutors.contains(executor)}

      if (preferredExecutors.isEmpty) {
        logDebug(s"No additional executors could be found matching placement constraints for failed task $taskIndex, allowing task to run unconstrained")
        unlaunchedUnconstrainedTasks += taskIndex
        currentLocalityWaitLevel(taskIndex) = (TaskLocality.ANY, clock.getTimeMillis())

        val targetExecutor = randomExecutor
        pendingUnconstrainedReservations(targetExecutor) += 1
        targetExecutor
      } else {
        val (targetExecutor, executorHost) = preferredExecutors(Random.nextInt(preferredExecutors.length))
        unlaunchedConstrainedTasks.addBinding(targetExecutor, taskIndex)
        pendingConstrainedTaskReservations.addBinding(taskIndex, targetExecutor)

        currentLocalityWaitLevel(taskIndex) = (getMatchedLocality(task, targetExecutor, executorHost), clock.getTimeMillis())

        targetExecutor
      }
    }

    pendingReservations(executor) += 1
    Map(executor -> 1)
  }

  //TODO merge with replacementReservations and getReservations
  def checkLocalityTimeout(): Map[String, Int] = {
    //for flare, we'll use a default of 10s instead of 3s
    val defaultWait = conf.get("spark.locality.wait", "10s")

    val reservations = new mutable.ListBuffer[String]
    val currentTime = clock.getTimeMillis()

    currentLocalityWaitLevel.foreach {
      case (taskIndex, (locality, localityStartTime)) => {
        val elapsedTime = currentTime - localityStartTime
        val triedExecutors = pendingConstrainedTaskReservations.getOrElse(taskIndex, Set.empty[String]).union(failedExecutors.get(taskIndex).map(_.keySet).getOrElse(Set.empty))

        locality match {
          case TaskLocality.PROCESS_LOCAL => {
            val localityWaitTime = conf.getTimeAsMs("spark.locality.wait.process", defaultWait)
            if (elapsedTime > localityWaitTime) {
              val task = tasks(taskIndex)

              val untriedExecutors = task.preferredLocations.flatMap {
                case ExecutorCacheTaskLocation(host, executorId) => Some(executorId)
                case _ => None
              }.toSet.diff(triedExecutors)

              if (!untriedExecutors.isEmpty) {
                val targetExecutor = untriedExecutors.head
                reservations += targetExecutor
                unlaunchedConstrainedTasks.addBinding(targetExecutor, taskIndex)
                pendingConstrainedTaskReservations.addBinding(taskIndex, targetExecutor)

                log.debug(s"Locality timed out on PROCESS_LOCAL task $taskIndex in stage ${taskSet.stageId}.${taskSet.stageAttemptId}, locality remaining at PROCESS_LOCAL, found untried executor ${targetExecutor}")
                currentLocalityWaitLevel(taskIndex) = (TaskLocality.PROCESS_LOCAL, clock.getTimeMillis())
              } else {
                val preferredHostsIterator = task.preferredLocations.map(_.host).distinct.iterator
                var selectedExecutor: Option[(String, String)] = None
                while (selectedExecutor.isEmpty && preferredHostsIterator.hasNext) {
                  val host = preferredHostsIterator.next
                  val untriedHostExecutors = scheduler.executorsByHost(host).toSet.diff(triedExecutors)
                  if (!untriedHostExecutors.isEmpty) {
                    selectedExecutor = Some((untriedHostExecutors.head, host))
                  }
                }
                selectedExecutor match {
                  case Some((executor, host)) => {
                    reservations += executor
                    unlaunchedConstrainedTasks.addBinding(executor, taskIndex)
                    pendingConstrainedTaskReservations.addBinding(taskIndex, executor)

                    log.debug(s"Locality timed out on PROCESS_LOCAL task $taskIndex in stage ${taskSet.stageId}.${taskSet.stageAttemptId}, locality downgraded to NODE_LOCAL, submitted reservation to executor $executor on host $host")

                    currentLocalityWaitLevel(taskIndex) = (TaskLocality.NODE_LOCAL, clock.getTimeMillis())
                  }
                  case None => {
                    localityLostTasks += taskIndex
                    currentLocalityWaitLevel(taskIndex) = (TaskLocality.ANY, clock.getTimeMillis())

                    val potentialExecutors = scheduler.executors.diff(triedExecutors.toSeq)
                    if (potentialExecutors.isEmpty) {
                      log.debug(s"Locality timed out on PROCESS_LOCAL task $taskIndex in stage ${taskSet.stageId}.${taskSet.stageAttemptId}, locality downgraded to ANY, no new reservations, task has been submitted to all executors")
                    } else {
                      val executor = potentialExecutors(Random.nextInt(potentialExecutors.length))
                      reservations += executor
                      pendingUnconstrainedReservations(executor) += 1

                      log.debug(s"Locality timed out on PROCESS_LOCAL task $taskIndex in stage ${taskSet.stageId}.${taskSet.stageAttemptId}, locality downgraded to ANY, submitting extra reservation to executor $executor")
                    }
                  }
                }
              }
            }
          }
          case TaskLocality.NODE_LOCAL => {
            val localityWaitTime = conf.getTimeAsMs("spark.locality.wait.node", defaultWait)
            if (elapsedTime > localityWaitTime) {
              val task = tasks(taskIndex)

              val preferredHostsIterator = task.preferredLocations.map(_.host).distinct.iterator
              var selectedExecutor: Option[(String, String)] = None
              while (selectedExecutor.isEmpty && preferredHostsIterator.hasNext) {
                val host = preferredHostsIterator.next
                val untriedHostExecutors = scheduler.executorsByHost(host).toSet.diff(triedExecutors)
                if (!untriedHostExecutors.isEmpty) {
                  selectedExecutor = Some((untriedHostExecutors.head, host))
                }
              }

              selectedExecutor match {
                case Some((executor, host)) => {
                  reservations += executor
                  unlaunchedConstrainedTasks.addBinding(executor, taskIndex)
                  pendingConstrainedTaskReservations.addBinding(taskIndex, executor)

                  log.debug(s"Locality timed out on NODE_LOCAL task $taskIndex in stage ${taskSet.stageId}.${taskSet.stageAttemptId}, locality remaining at NODE_LOCAL, found untried executor $executor on host $host")

                  currentLocalityWaitLevel(taskIndex) = (TaskLocality.NODE_LOCAL, clock.getTimeMillis())
                }
                case None => {
                  localityLostTasks += taskIndex
                  currentLocalityWaitLevel(taskIndex) = (TaskLocality.ANY, clock.getTimeMillis())

                  val potentialExecutors = scheduler.executors.diff(triedExecutors.toSeq)
                  if (potentialExecutors.isEmpty) {
                    log.debug(s"Locality timed out on NODE_LOCAL task $taskIndex in stage ${taskSet.stageId}.${taskSet.stageAttemptId}, locality downgraded to ANY, no new reservations, task has been submitted to all executors")
                  } else {
                    val executor = potentialExecutors(Random.nextInt(potentialExecutors.length))
                    reservations += executor
                    pendingUnconstrainedReservations(executor) += 1

                    log.debug(s"Locality timed out on NODE_LOCAL task $taskIndex in stage ${taskSet.stageId}.${taskSet.stageAttemptId}, locality downgraded to ANY, submitting extra reservation to executor $executor")
                  }
                }
              }
            }
          }
          case _ =>
        }
      }
    }
    val reservationCounts = reservations.groupBy(identity).mapValues(_.size)
    pendingReservations ++= reservationCounts.map { case (k, v) => k -> (v + pendingReservations.getOrElse(k, 0)) }

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

  def abort(message: String, exception: Option[Throwable] = None) = scheduler.synchronized {
    scheduler.dagScheduler.taskSetFailed(taskSet, message, exception)
    isZombie = true
    maybeFinishTaskSet()
  }


  def canFetchMoreResults(size: Long): Boolean = scheduler.synchronized {
    totalResultSize += size
    calculatedTasks += 1
    if (maxResultSize > 0 && totalResultSize > maxResultSize) {
      val msg = s"Total size of serialized results of ${calculatedTasks} tasks " +
        s"(${Utils.bytesToString(totalResultSize)}) is bigger than spark.driver.maxResultSize " +
        s"(${Utils.bytesToString(maxResultSize)})"
      logError(msg)
      abort(msg)
      false
    } else {
      true
    }
  }

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
      tasks(index), Success, result.value(), result.accumUpdates, taskInfo)

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

    var accumUpdates: Seq[AccumulatorV2[_, _]] = Seq.empty
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
        accumUpdates = ef.accums
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

    failedExecutors.getOrElseUpdate(index, new mutable.HashMap[String, Long]()).put(taskInfo.executorId, clock.getTimeMillis())

    scheduler.dagScheduler.taskEnded(tasks(index), reason, null, accumUpdates, taskInfo)

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
    replacementReservations(index)
  }

  def isMaxParallelism: Boolean = parallelismLimit.fold(false)(runningTaskCount >= _)

  def getTask(executorId: String, host: String): Option[TaskDescription] = {
    pendingReservations(executorId) -= 1

    val currentTime = clock.getTimeMillis()
    val selectedTask: Option[Int] = {
      if (unlaunchedConstrainedTasks.contains(executorId)) {
        val index = unlaunchedConstrainedTasks(executorId).head

        pendingConstrainedTaskReservations.remove(index).foreach(
          _.foreach(unlaunchedConstrainedTasks.removeBinding(_, index)))

        localityLostTasks.remove(index)
        currentLocalityWaitLevel.remove(index)

        Some(index)
      } else {
        if (pendingUnconstrainedReservations(executorId) > 0)
          pendingUnconstrainedReservations(executorId) -= 1

        if (!unlaunchedUnconstrainedTasks.isEmpty) {
          Some(unlaunchedUnconstrainedTasks.remove(0))
        } else if (!localityLostTasks.isEmpty) {
          val index = localityLostTasks.head

          log.debug(s"No unconstrained tasks are pending for stage ${taskSet.stageId}.${taskSet.stageAttemptId}, running locality timed out constrained task $index on executor $executorId at ANY level")

          pendingConstrainedTaskReservations.remove(index).foreach(
            _.foreach(unlaunchedConstrainedTasks.removeBinding(_, index)))

          localityLostTasks.remove(index)
          currentLocalityWaitLevel.remove(index)

          Some(index)
        } else {
          None
        }
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

  def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Map[String, Int] = {
    var reservations = mutable.HashMap[String, Int]().withDefaultValue(0)

    unlaunchedConstrainedTasks(executorId).foreach { index =>
      reservations ++ replacementReservations(index).map { case (e, r) => e -> (r + reservations(e))}
    }

    if (pendingUnconstrainedReservations(executorId) > 0) {
      val replacementExecutor = randomExecutor
      pendingUnconstrainedReservations(replacementExecutor) += 1
      reservations(replacementExecutor) += 1
    }

    // Re-enqueue any tasks that ran on the failed executor if this is a shuffle map stage,
    // and we are not using an external shuffle server which could serve the shuffle outputs.
    // The reason is the next stage wouldn't be able to fetch the data from this dead executor
    // so we would need to rerun these tasks on other executors.
    if (tasks(0).isInstanceOf[ShuffleMapTask] && !env.blockManager.externalShuffleServiceEnabled) {
      for ((taskId, info) <- taskInfos if info.executorId == executorId) {
        val index = taskInfos(taskId).index
        if (successful(index)) {
          successful(index) = false
          tasksSuccessful -= 1
          reservations ++ replacementReservations(index).map { case (e, r) => e -> (r + reservations(e))}
          // Tell the DAGScheduler that this task was resubmitted so that it doesn't think our
          // stage finishes when a total of tasks.size tasks finish.
          scheduler.dagScheduler.taskEnded(tasks(index), Resubmitted, null, Seq.empty, info)
        }
      }
    }

    for ((taskId, info) <- taskInfos if info.running && info.executorId == executorId) {
      val exitCausedByApp: Boolean = reason match {
        case exited: ExecutorExited => exited.exitCausedByApp
        case ExecutorKilled => false
        case _ => true
      }
      reservations ++= handleFailedTask(taskId, TaskState.FAILED, ExecutorLostFailure(info.executorId, exitCausedByApp,
        Some(reason.toString))).map { case (e, r) => e -> (r + reservations(e)) }
    }

    reservations.toMap
  }
}