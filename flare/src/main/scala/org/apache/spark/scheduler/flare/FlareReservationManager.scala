package org.apache.spark.scheduler.flare

import java.nio.ByteBuffer

import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.util.{Clock, SystemClock, Utils}
import org.apache.spark._

import scala.collection.mutable.{HashMap, HashSet, ListBuffer, MultiMap, Set}
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
  
  val maxResultSize = Utils.getMaxResultSize(conf)
  
  val tasks = taskSet.tasks
  val numTasks = tasks.length
    
  val launchedConstrainedTasks = new HashMap[String, Int]
  
  val unlaunchedConstrainedTasks = new HashMap[String, Set[Int]] with MultiMap[String, Int]
  val unlaunchedUnconstrainedTasks = new ListBuffer[Int]
  
  val pendingReservations = new HashMap[String, Int]
  
  val runningTasksSet = new HashSet[Long]
  
  def runningTasks: Int = runningTasksSet.size
  
  val taskInfos = new HashMap[Long, TaskInfo]
  
  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)
  var tasksSuccessful = 0

  val epoch = scheduler.mapOutputTracker.getEpoch
  logDebug("Epoch for " + taskSet + ": " + epoch)
  for (t <- tasks) {
    t.epoch = epoch
  }
  
  def addRunningTask(taskId: Long) {
    runningTasksSet.add(taskId)
  }
  
  def removeRunningTask(taskId: Long) {
    runningTasksSet.remove(taskId)
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

    reservations.groupBy(identity).mapValues(_.size)
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
    //scheduler.taskSetFinished(this)
    
  }
  
  def executorLost(executorId: String, host: String, reason: ExecutorLossReason)  = {
    
  }
  
  def canFetchMoreResults(size: Long): Boolean = true
  
  def handleTaskGettingResult(taskId: Long): Unit = {
    val info = taskInfos(taskId)
    info.markGettingResult()
    scheduler.dagScheduler.taskGettingResult(info)
  }
  
  def handleSuccessfulTask(taskId: Long, result: DirectTaskResult[_]): Unit = {
    val taskInfo = taskInfos(taskId)
    val index = taskInfo.index
    taskInfo.markSuccessful()
    removeRunningTask(taskId)
    
    scheduler.dagScheduler.taskEnded(
      tasks(index), Success, result.value(), result.accumUpdates, taskInfo, result.metrics)
      
    tasksSuccessful += 1  
    logInfo("Finished task %s in stage %s (TID %d) in %d ms on %s (%d/%d)".format(
        taskInfo.id, taskSet.id, taskInfo.taskId, taskInfo.duration, taskInfo.host, tasksSuccessful, numTasks))

  }

  
  def handleFailedTask(taskId: Long, state: TaskState, reason: TaskEndReason): Unit = {
    val taskInfo = taskInfos(taskId)
    val index = taskInfo.index
    if (taskInfo.failed) {
      return
    }
    removeRunningTask(taskId)
    taskInfo.markFailed()
    
    val taskMetrics: TaskMetrics = null
    
    scheduler.dagScheduler.taskEnded(tasks(index), reason, null, null, taskInfo, taskMetrics)
  }

  def isMaxParallelism: Boolean = parallelismLimit.fold(false)(runningTasks >= _)
   
  def getTask(executorId: String, host: String): Option[TaskDescription] = {
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