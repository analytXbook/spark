package org.apache.spark.deploy.flare

import java.io.{File, IOException}
import java.util.UUID

import org.apache.spark.deploy.ExecutorState

import scala.collection.mutable.HashMap
import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.flare.{FlareCluster, _}
import org.apache.spark.util.SignalLogger

private[spark] class FlareNode(
    args: FlareNodeArguments,
    workDirPath: Option[String])
  extends FlareClusterListener with Logging {
  
  private val testing: Boolean = sys.props.contains("spark.testing")
  private val sparkHome =
    if (testing) {
      assert(sys.props.contains("spark.test.home"), "spark.test.home is not set!")
      new File(sys.props("spark.test.home"))
    } else {
      new File(sys.env.get("SPARK_HOME").getOrElse(".."))
    }
  
  val executors = new HashMap[String, FlareExecutorRunner]

  val clusterConf = args.clusterConf
  val cluster = FlareCluster(clusterConf)
  var executorIdCounter: FlareCounter = _

  var workDir: File = null

  val securityManager = new SecurityManager(new SparkConf)

  val nodeId = UUID.randomUUID().toString
  
  private def createWorkDir() {
    workDir = workDirPath.map(new File(_)).getOrElse(new File(sparkHome, "work"))
    try {
      workDir.mkdirs()
      if ( !workDir.exists() || !workDir.isDirectory) {
        logError("Failed to create work directory " + workDir)
        System.exit(1)
      }
      assert (workDir.isDirectory)
    } catch {
      case e: Exception =>
        logError("Failed to create work directory " + workDir, e)
        System.exit(1)
    }
  }
  
  private def joinCluster() = {
    cluster.addListener(this)
    cluster.start(NodeClusterProfile(nodeId, args.hostname) )
    executorIdCounter = cluster.counter("executorId")
  }

  override def onDriverExited(data: DriverData): Unit = {
    if (cluster.drivers.isEmpty) {
      logInfo("All drivers have exited, terminating executors and uninitializing cluster")
      cluster.reset()
      terminateExecutors()
    }
  }

  private def nextExecutorId(): Int = executorIdCounter.incrementAtomic().toInt
  
  private def launchExecutors() = {
    logInfo(s"Configuration: ${cluster.properties}")
    val conf = new SparkConf()
    cluster.properties.foreach {
      case (key, value) => conf.set(key, value)
    }

    val executorCount = conf.getInt("spark.flare.executorsPerNode", 1)

    logInfo(s"Launching $executorCount executors")
    for (i <- 0 until executorCount) {
      val executorId = nextExecutorId.toString
      launchExecutor(executorId, conf)
    }
  }
  
  private def launchExecutor(executorId: String, conf: SparkConf) = {
    val appDir = new File(workDir, cluster.appId)
    if (!appDir.exists() && !appDir.mkdir()) {
      log.warn("Failed to create directory " + appDir)
    }

    val executorDir = new File(appDir, executorId.toString)
    if (!executorDir.mkdirs()) {
      log.warn("Failed to create directory " + executorDir)
    }

    val memory = conf.getSizeAsMb("spark.executor.memory", "1g")
    val cores = conf.getInt("spark.executor.cores", 1)

    val executor = new FlareExecutorRunner(clusterConf, executorId, cores, memory, sparkHome, executorDir, conf, securityManager, this)
    executor.start()
    executors(executorId) = executor
  }

  private def terminateExecutors() = {
    for (executor <- executors.values) {
      logInfo(s"Terminating executor ${executor.executorId}")
      executor.kill()
    }
  }

  def onExecutorStateChanged(executorId: String, state: ExecutorState.Value, message: Option[String], exitCode: Option[Int]): Unit = {
    /*
    import ExecutorState._
    state match {
      case KILLED | FAILED | LOST | EXITED => {
        executors.remove(executorId)
        cluster.send(FlareExecutorLost(executorId, System.currentTimeMillis, s"Executor $state: $message"))
      }
      case _ =>
    }*/
  }

  def start(): Unit = {
    logInfo("Starting Flare Node")
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    createWorkDir()
    joinCluster()
    launchExecutors() 
  }

  def close() = {
    cluster.close()
  }
}

object FlareNode extends Logging {
  def main(args: Array[String]) = {
    SignalLogger.register(log)
    
    val nodeArgs = new FlareNodeArguments(args)
    
    val node = new FlareNode(nodeArgs, Option(nodeArgs.workDir))

    node.start()
  }
}

