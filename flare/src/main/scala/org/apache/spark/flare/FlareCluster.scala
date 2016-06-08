package org.apache.spark.flare

import org.jgroups.JChannel
import org.apache.spark.{Logging, SparkConf}
import org.jgroups.Channel
import java.io.OutputStream

import org.jgroups.View
import java.io.InputStream

import org.jgroups.Message
import org.jgroups.ReceiverAdapter
import org.jgroups.util.Util
import java.io.DataOutputStream
import java.io.DataInputStream
import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit

import org.apache.spark.util.ThreadUtils
import org.jgroups.Address
import org.jgroups.blocks.atomic.CounterService
import org.jgroups.blocks.atomic.Counter
import org.jgroups.protocols._
import org.jgroups.stack.{IpAddress, ProtocolStack}

import scala.collection.JavaConverters._
import scala.collection.mutable

private[spark] class FlareCluster(val conf: FlareClusterConfiguration) extends ReceiverAdapter with Logging {
  private val listenerBus = new FlareClusterListenerBus
  
  val state: FlareClusterState = new FlareClusterState
  
  private val channel: Channel = createChannel
  private val counterService: CounterService = new CounterService(channel)

  private var lastView: View = _

  private val printScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("counter-print-scheduler")
  printScheduler.scheduleAtFixedRate(new Runnable {
    def run() = {
      logDebug("Cluster Counters:\n" + counterService.printCounters())
    }
  }, 1000, 1000, TimeUnit.MILLISECONDS)

  private def createProtocolStack: ProtocolStack = {
    new ProtocolStack()
      .addProtocol(new TCP_NIO2()
        .setValue("bind_addr", InetAddress.getByName(conf.bindHostname))
        .setValue("bind_port", conf.bindPort)
        .setValue("port_range", conf.portRange))
      .addProtocol(new DAISYCHAIN())
      .addProtocol(new TCPPING()
        .setValue("initial_hosts", conf.initialMembers.map { case (host, port) => new IpAddress(host, port) }.asJava)
        .setValue("send_cache_on_join", true))
      .addProtocol(new MERGE3())
      .addProtocol(new FD_SOCK())
      .addProtocol(new FD())
      .addProtocol(new VERIFY_SUSPECT())
      .addProtocol(new BARRIER())
      .addProtocol(new pbcast.NAKACK2()
        .setValue("use_mcast_xmit", false))
      .addProtocol(new UNICAST3())
      .addProtocol(new pbcast.STABLE())
      .addProtocol(new pbcast.GMS()
        .setValue("print_local_addr", false))
      .addProtocol(new MFC())
      .addProtocol(new FRAG2())
      .addProtocol(new COUNTER())
      .addProtocol(new pbcast.STATE_TRANSFER())
  }

  private def createChannel: Channel = {
    val channel = new JChannel()

    channel.setReceiver(this)

    val stack = createProtocolStack
    channel.setProtocolStack(stack)
    stack.init()

    channel
  }

  def connect(): Unit = {
    listenerBus.start(null)

    channel.connect(conf.channelName, null, 60000)
  }

  override def viewAccepted(view: View): Unit = {
    val lostMembers = View.diff(lastView, view)(1)
    lostMembers.foreach { address =>
      state.driverAddress.get(address).foreach { driverId => 
        logInfo(s"Driver exited: $driverId")

        state.synchronized {
          state.drivers -= driverId
          state.driverAddress -= address
        }
        listenerBus.post(DriverExited(driverId))
      }
    }
    lastView = view
  }  
    
  override def receive(msg: Message): Unit = {
    Util.objectFromByteBuffer(msg.getRawBuffer) match {
      case NodeJoined(executorCount) => state.synchronized {
        logInfo("Node Joined")
        state.nodes += (msg.src -> FlareNodeInfo(executorCount))
      }

      case executorLaunched @ ExecutorLaunched(executorId, hostname, port) => state.synchronized {
        logInfo(s"Executor Launched: $executorId")
        state.executors += (executorId -> FlareExecutorInfo(hostname, port))
        listenerBus.post(executorLaunched)
      }

      case executorLost @ FlareExecutorLost(executorId, time, reason) => state.synchronized {
        logInfo(s"Executor $executorId lost: $reason")
        state.executors -= executorId
        listenerBus.post(executorLost)
      }
      
      case driverJoined @ DriverJoined(driverId, hostname, port: Int) => state.synchronized {
        logInfo(s"Driver joined: $driverId")
        state.drivers += (driverId -> FlareDriverInfo(hostname, port))
        state.driverAddress += (msg.src -> driverId)
        listenerBus.post(driverJoined)
      }
      case initialize @ Initialize(appId, properties) => state.synchronized {
        logInfo(s"Driver initialized cluster")
        state.initialize(appId, properties)
        listenerBus.post(initialize)
      }
    }
  }
  
  override def getState(output: OutputStream): Unit = {
    state.synchronized {
      Util.objectToStream(state, new DataOutputStream(output))
    }
  }
   
  override def setState(input: InputStream): Unit = {
    val stateUpdate = Util.objectFromStream(new DataInputStream(input)).asInstanceOf[FlareClusterState]

    state.load(stateUpdate)
  }
  
  def counter(name: String, initialValue: Long = 0l): Counter =
    counterService.getOrCreateCounter(name, initialValue)

  private val asyncCounters = new mutable.HashMap[String, AsyncCounter]

  def asyncCounter(name: String, initialValue: Long = 0l): AsyncCounter =
    asyncCounters.getOrElseUpdate(name, new AsyncCounter(counter(name, initialValue)))

  def addListener(listener: FlareClusterListener) = {
    listenerBus.addListener(listener)
  }
     
  def send(event: FlareClusterEvent): Unit = {
    channel.send(null, event)
  }
  
  def send(address: Address, event: FlareClusterEvent): Unit = {
    channel.send(address, event)
  }
 
  def close(): Unit = {
    channel.close()
    listenerBus.stop()
  }
}

object FlareCluster {
  def apply(conf: FlareClusterConfiguration) = new FlareCluster(conf)
  def apply(url: String, conf: SparkConf) = new FlareCluster(FlareClusterConfiguration.fromUrl(url, conf))
}

