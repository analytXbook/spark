package org.apache.spark.scheduler.flare

import org.apache.spark._
import org.apache.spark.util.ThreadUtils
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.scheduler._
import java.util.concurrent.RejectedExecutionException
import scala.util.control.NonFatal
import java.nio.ByteBuffer
import org.apache.spark.TaskState.TaskState
import org.apache.spark.util.Utils
import scala.language.existentials


class FlareTaskResultGetter(
    sparkEnv: SparkEnv, 
    scheduler: FlareScheduler) 
  extends Logging {
  
  private val THREADS = sparkEnv.conf.getInt("spark.resultGetter.threads", 4)
  private val getTaskResultExecutor = ThreadUtils.newDaemonFixedThreadPool(
    THREADS, "task-result-getter")
  
  protected val serializer = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      sparkEnv.closureSerializer.newInstance()
    }
  }
  
  def enqueueSuccessfulTask(
    reservationManager: FlareReservationManager, taskId: Long, serializedData: ByteBuffer) {
    getTaskResultExecutor.execute(new Runnable {
      override def run(): Unit = Utils.logUncaughtExceptions {
        try {
          val (result, size) = serializer.get().deserialize[TaskResult[_]](serializedData) match {
            case directResult: DirectTaskResult[_] =>
              if (!reservationManager.canFetchMoreResults(serializedData.limit())) {
                return
              }
              // deserialize "value" without holding any lock so that it won't block other threads.
              // We should call it here, so that when it's called again in
              // "TaskSetManager.handleSuccessfulTask", it does not need to deserialize the value.
              directResult.value()
              (directResult, serializedData.limit())
            case IndirectTaskResult(blockId, size) =>
              if (!reservationManager.canFetchMoreResults(size)) {
                // dropped by executor if size is larger than maxResultSize
                sparkEnv.blockManager.master.removeBlock(blockId)
                return
              }
              logDebug("Fetching indirect task result for TID %s".format(taskId))
              scheduler.handleTaskGettingResult(reservationManager, taskId)
              val serializedTaskResult = sparkEnv.blockManager.getRemoteBytes(blockId)
              if (!serializedTaskResult.isDefined) {
                /* We won't be able to get the task result if the machine that ran the task failed
                 * between when the task ended and when we tried to fetch the result, or if the
                 * block manager had to flush the result. */
                scheduler.handleFailedTask(
                  reservationManager, taskId, TaskState.FINISHED, TaskResultLost)
                return
              }
              val deserializedResult = serializer.get().deserialize[DirectTaskResult[_]](
                serializedTaskResult.get)
              sparkEnv.blockManager.master.removeBlock(blockId)
              (deserializedResult, size)
          }

          result.metrics.setResultSize(size)
          scheduler.handleSuccessfulTask(reservationManager, taskId, result)
        } catch {
          case cnf: ClassNotFoundException =>
            val loader = Thread.currentThread.getContextClassLoader
            reservationManager.abort("ClassNotFound with classloader: " + loader)
          // Matching NonFatal so we don't catch the ControlThrowable from the "return" above.
          case NonFatal(ex) =>
            logError("Exception while getting task result", ex)
            reservationManager.abort("Exception while getting task result: %s".format(ex))
        }
      }
    })
  }

  def enqueueFailedTask(reservationManager: FlareReservationManager, tid: Long, taskState: TaskState,
    serializedData: ByteBuffer) {
    var reason : TaskEndReason = UnknownReason
    try {
      getTaskResultExecutor.execute(new Runnable {
        override def run(): Unit = Utils.logUncaughtExceptions {
          val loader = Utils.getContextOrSparkClassLoader
          try {
            if (serializedData != null && serializedData.limit() > 0) {
              reason = serializer.get().deserialize[TaskEndReason](
                serializedData, loader)
            }
          } catch {
            case cnd: ClassNotFoundException =>
              // Log an error but keep going here -- the task failed, so not catastrophic
              // if we can't deserialize the reason.
              logError(
                "Could not deserialize TaskEndReason: ClassNotFound with classloader " + loader)
            case ex: Exception => {}
          }
          scheduler.handleFailedTask(reservationManager, tid, taskState, reason)
        }
      })
    } catch {
      case e: RejectedExecutionException if sparkEnv.isStopped =>
        // ignore it
    }
  }

  def stop() {
    getTaskResultExecutor.shutdownNow()
  }
  
  

}