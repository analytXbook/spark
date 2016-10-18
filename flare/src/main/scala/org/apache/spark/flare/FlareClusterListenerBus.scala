package org.apache.spark.flare

import java.util.concurrent.{TimeoutException, Semaphore, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.SparkContext
import org.apache.spark.util.{Utils, ListenerBus}

import scala.util.DynamicVariable

private[spark] class FlareClusterListenerBus extends ListenerBus[FlareClusterListener, FlareClusterEvent] {

  self =>

  import FlareClusterListenerBus._

  override def doPostEvent(listener: FlareClusterListener, event: FlareClusterEvent): Unit = {
    event match {
      case FlareExecutorExited(data) => listener.onExecutorExited(data)
      case FlareExecutorJoined(data) => listener.onExecutorJoined(data)
      case FlareExecutorUpdated(data) => listener.onExecutorUpdated(data)
      case FlareDriverExited(data) => listener.onDriverExited(data)
      case FlareDriverJoined(data) => listener.onDriverJoined(data)
      case FlareDriverUpdated(data) => listener.onDriverUpdated(data)
      case FlareNodeExited(data) => listener.onNodeExited(data)
      case FlareNodeJoined(data) => listener.onNodeJoined(data)
      case FlareNodeUpdated(data) => listener.onNodeUpdated(data)
    }
  }

  private var sparkContext: SparkContext = null

  // Cap the capacity of the event queue so we get an explicit error (rather than
  // an OOM exception) if it's perpetually being added to more quickly than it's being drained.
  private val EVENT_QUEUE_CAPACITY = 10000
  private val eventQueue = new LinkedBlockingQueue[FlareClusterEvent](EVENT_QUEUE_CAPACITY)

  // Indicate if `start()` is called
  private val started = new AtomicBoolean(false)
  // Indicate if `stop()` is called
  private val stopped = new AtomicBoolean(false)

  // Indicate if we are processing some event
  // Guarded by `self`
  private var processingEvent = false

  private val logDroppedEvent = new AtomicBoolean(false)

  // A counter that represents the number of events produced and consumed in the queue
  private val eventLock = new Semaphore(0)

  private val listenerThread = new Thread(name) {
    setDaemon(true)
    override def run(): Unit = Utils.tryOrStopSparkContext(sparkContext) {
      FlareClusterListenerBus.withinListenerThread.withValue(true) {
        while (true) {
          eventLock.acquire()
          self.synchronized {
            processingEvent = true
          }
          try {
            val event = eventQueue.poll
            if (event == null) {
              // Get out of the while loop and shutdown the daemon thread
              if (!stopped.get) {
                throw new IllegalStateException("Polling `null` from eventQueue means" +
                  " the listener bus has been stopped. So `stopped` must be true")
              }
              return
            }
            postToAll(event)
          } finally {
            self.synchronized {
              processingEvent = false
            }
          }
        }
      }
    }
  }

  /**
    * Start sending events to attached listeners.
    *
    * This first sends out all buffered events posted before this listener bus has started, then
    * listens for any additional events asynchronously while the listener bus is still running.
    * This should only be called once.
    *
    * @param sc Used to stop the SparkContext in case the listener thread dies.
    */
  def start(sc: SparkContext): Unit = {
    if (started.compareAndSet(false, true)) {
      sparkContext = sc
      listenerThread.start()
    } else {
      throw new IllegalStateException(s"$name already started!")
    }
  }

  def post(event: FlareClusterEvent): Unit = {
    if (stopped.get) {
      // Drop further events to make `listenerThread` exit ASAP
      logError(s"$name has already stopped! Dropping event $event")
      return
    }
    val eventAdded = eventQueue.offer(event)
    if (eventAdded) {
      eventLock.release()
    } else {
      onDropEvent(event)
    }
  }

  /**
    * For testing only. Wait until there are no more events in the queue, or until the specified
    * time has elapsed. Throw `TimeoutException` if the specified time elapsed before the queue
    * emptied.
    * Exposed for testing.
    */
  @throws(classOf[TimeoutException])
  def waitUntilEmpty(timeoutMillis: Long): Unit = {
    val finishTime = System.currentTimeMillis + timeoutMillis
    while (!queueIsEmpty) {
      if (System.currentTimeMillis > finishTime) {
        throw new TimeoutException(
          s"The event queue is not empty after $timeoutMillis milliseconds")
      }
      /* Sleep rather than using wait/notify, because this is used only for testing and
       * wait/notify add overhead in the general case. */
      Thread.sleep(10)
    }
  }

  /**
    * For testing only. Return whether the listener daemon thread is still alive.
    * Exposed for testing.
    */
  def listenerThreadIsAlive: Boolean = listenerThread.isAlive

  /**
    * Return whether the event queue is empty.
    *
    * The use of synchronized here guarantees that all events that once belonged to this queue
    * have already been processed by all attached listeners, if this returns true.
    */
  private def queueIsEmpty: Boolean = synchronized { eventQueue.isEmpty && !processingEvent }

  /**
    * Stop the listener bus. It will wait until the queued events have been processed, but drop the
    * new events after stopping.
    */
  def stop(): Unit = {
    if (!started.get()) {
      throw new IllegalStateException(s"Attempted to stop $name that has not yet started!")
    }
    if (stopped.compareAndSet(false, true)) {
      // Call eventLock.release() so that listenerThread will poll `null` from `eventQueue` and know
      // `stop` is called.
      eventLock.release()
      listenerThread.join()
    } else {
      // Keep quiet
    }
  }

  /**
    * If the event queue exceeds its capacity, the new events will be dropped. The subclasses will be
    * notified with the dropped events.
    *
    * Note: `onDropEvent` can be called in any thread.
    */
  def onDropEvent(event: FlareClusterEvent): Unit = {
    if (logDroppedEvent.compareAndSet(false, true)) {
      // Only log the following message once to avoid duplicated annoying logs.
      logError("Dropping FlareClusterEvent because no remaining room in event queue. " +
        "This likely means one of the FlareListeners is too slow and cannot keep up with " +
        "the rate at which tasks are being started by the scheduler.")
    }
  }
}

private[spark] object FlareClusterListenerBus {
  // Allows for Context to check whether stop() call is made within listener thread
  val withinListenerThread: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)

  /** The thread name of Spark listener bus */
  val name = "FlareClusterListenerBus"
}