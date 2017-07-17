package org.apache.spark.deploy.flare


import java.io._
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import com.google.common.io.Files

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages.ExecutorStateChanged
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.util.logging.FileAppender
import org.apache.spark.deploy.{Command, ExecutorState}
import org.apache.spark.deploy.worker.CommandUtils

import org.apache.spark.flare.FlareClusterConfiguration

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


  @volatile var state: ExecutorState.Value = ExecutorState.LAUNCHING

  private def getCommand(): Command = {
    val args = Seq(
      "--executor-id", executorId,
      "--hostname", clusterConf.hostname,
      "--cores", cores.toString,
      "--redis-host", node.args.redisHost,
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
  }

  private def killProcess(message: Option[String]) = {
    if (process != null) {
      logInfo(s"Killing process for executor $executorId ")
      if (stdoutAppender != null) {
        stdoutAppender.stop()
      }
      if (stderrAppender != null) {
        stderrAppender.stop()
      }

      process.destroy()

      val exitCode = Utils.terminateProcess(process, EXECUTOR_TERMINATE_TIMEOUT_MS)

      node.onExecutorStateChanged(executorId, state, message, exitCode)
    }
  }
  
  def kill() = {
    if (workerThread != null) {
      workerThread.interrupt()
      workerThread = null
      state = ExecutorState.KILLED
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
      Files.write(header, stderr, StandardCharsets.UTF_8)
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