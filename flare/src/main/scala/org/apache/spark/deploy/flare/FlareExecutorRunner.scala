package org.apache.spark.deploy.flare

import java.io.File

import scala.collection.JavaConverters._
import com.google.common.base.Charsets.UTF_8
import com.google.common.io.Files
import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.deploy.{Command, ExecutorState}
import org.apache.spark.deploy.worker.CommandUtils
import org.apache.spark.flare.FlareClusterConfiguration
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.util.logging.FileAppender

import scala.collection.mutable.HashMap

private[spark] class FlareExecutorRunner(
    val clusterConf: FlareClusterConfiguration,
    val executorId: String,
    val cores: Int,
    val memory: Long,
    val sparkHome: File,
    val executorDir: File,
    conf: SparkConf,
    securityManager: SecurityManager,
    node: FlareNode)
  extends Logging{
  private var workerThread: Thread = _
  private var process: Process = _
  private var stdoutAppender: FileAppender = _
  private var stderrAppender: FileAppender = _ 
  
  private val EXECUTOR_TERMINATE_TIMEOUT_MS = 15 * 1000

  private var shutdownHook: AnyRef = _

  @volatile var state: ExecutorState.Value = ExecutorState.LAUNCHING

  private def getCommand(): Command = {
    val args = Seq(
      "--executor-id", executorId,
      "--hostname", clusterConf.hostname,
      "--cores", cores.toString,
      clusterConf.clusterUrl)
    
    val extraJavaOpts = conf.getOption("spark.executor.extraJavaOptions")
      .map(Utils.splitCommandString).getOrElse(Seq.empty)
    val classPathEntries = conf.getOption("spark.executor.extraClassPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)
    val libraryPathEntries = conf.getOption("spark.executor.extraLibraryPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)
    val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    
    val testingClassPath =
      if (sys.props.contains("spark.testing")) {
        sys.props("java.class.path").split(java.io.File.pathSeparator).toSeq
      } else {
        Nil
      }
    
    val classPath = classPathEntries ++ testingClassPath
    
    val envVars = HashMap[String, String]()
    
    Command("org.apache.spark.executor.flare.FlareExecutorBackend", args, envVars, classPath, libraryPathEntries, javaOpts)
  }
  
  def start() = {
    workerThread = new Thread("flare-executor-runner-" + executorId) {
      override def run() { fetchAndRunExecutor() }
    }
    workerThread.start()
    
    shutdownHook = ShutdownHookManager.addShutdownHook { () =>
      if (state == ExecutorState.RUNNING) {
        state = ExecutorState.FAILED
      }
      killProcess(Some("Flare executor shutting down"))
    }
  }

  private def killProcess(message: Option[String]) = {
    if (process != null) {
      logInfo("Killing process!")
      if (stdoutAppender != null) {
        stdoutAppender.stop()
      }
      if (stderrAppender != null) {
        stderrAppender.stop()
      }
      val exited = Utils.waitForProcess(process, EXECUTOR_TERMINATE_TIMEOUT_MS)
      if (exited) {
        logWarning("Failed to terminate process " + process +
          "gracefully. Destroying forcibly, this process may become orphaned.")
        process.destroyForcibly()
      }

      val exitCode = if (exited) Some(process.exitValue()) else None

      node.onExecutorStateChanged(executorId, state, message, exitCode)
    }
  }
  
  def kill() = {
    if (workerThread != null) {
      workerThread.interrupt()
      workerThread = null
      state = ExecutorState.KILLED
      
      try {
        ShutdownHookManager.removeShutdownHook(shutdownHook)
      } catch {
        case e: IllegalStateException => None
      }
    }
  }
  
  def substituteVariables(argument: String): String = argument 
  
  private def fetchAndRunExecutor() = {
    try {
      val builder = CommandUtils.buildProcessBuilder(getCommand(), securityManager, memory.toInt, sparkHome.getAbsolutePath, substituteVariables)
      val command = builder.command()
      val formattedCommand = command.asScala.mkString("\"", "\" \"", "\"")
      logInfo(s"Launch command: $formattedCommand")
      
      builder.directory(executorDir)
      
      builder.environment.put("SPARK_LAUNCH_WITH_SCALA", "0")

      process = builder.start()
      
      val header = "Spark Executor Command: %s\n%s\n\n".format(
        formattedCommand, "=" * 40)
        
      val stdout = new File(executorDir, "stdout")
      stdoutAppender = FileAppender(process.getInputStream, stdout, conf)
            
      val stderr = new File(executorDir, "stderr")
      Files.write(header, stderr, UTF_8)
      stderrAppender = FileAppender(process.getErrorStream, stderr, conf)
      
      val exitCode = process.waitFor()
      
      state = ExecutorState.EXITED
      val message = "Command exited with code " + exitCode

      node.onExecutorStateChanged(executorId, state, Some(message), Some(exitCode))
    } catch {
      case interrupted: InterruptedException => {
        logInfo("Runner thread for executor " + executorId + " interrupted")
        state = ExecutorState.KILLED
        killProcess(None)
      }
      case e: Exception => {
        logError("Error running executor", e)
        state = ExecutorState.FAILED
        killProcess(Some(e.toString))
      }
    }
  }
}