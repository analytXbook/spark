package org.apache.spark.scheduler.flare

import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}
import org.apache.spark.scheduler.cluster.ExecutorInfo

class FlareExecutorInfo(
  val executorEndpoint: RpcEndpointRef,
  executorHost: String,
  totalCores: Int,
  logUrlMap: Map[String, String]) extends ExecutorInfo(executorHost, totalCores, logUrlMap)