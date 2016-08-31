package org.apache.spark.flare

import org.apache.curator.framework.recipes.barriers.DistributedBarrier
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}
import org.apache.spark.executor.flare.FlarePoolBackend
import org.apache.spark.{Logging, SparkException}
import org.apache.zookeeper.CreateMode
import org.apache.spark.util.SerializerUtils._

sealed trait FlareClusterProfile {
  def start(cluster: FlareCluster): Unit
  def reset(cluster: FlareCluster): Unit =
    throw new UnsupportedOperationException("Only nodes can reset the cluster state")
}

case class NodeClusterProfile(nodeId: String, hostname: String, poolBackend: FlarePoolBackend) extends FlareClusterProfile with Logging {
  override def start(cluster: FlareCluster): Unit = {
    val zk = cluster.zk
    val initializationBarrier = new DistributedBarrier(zk, "/init/barrier")
    if (cluster.drivers.isEmpty) {
      logInfo("No drivers found")
      initializationBarrier.setBarrier()

      logInfo("Resetting pool backend")
      poolBackend.reset()

      if (cluster.nodes.isEmpty) {
        if (zk.checkExists.forPath("/reset") != null) {
          logInfo("Cleaning up previous cluster state")
          zk.delete().deletingChildrenIfNeeded().forPath("/reset")
        }
      }
    }

    logInfo("Waiting for cleanup on previous app")
    val resetBarrier = new DistributedBarrier(zk, "/reset/barrier")
    resetBarrier.waitOnBarrier()

    cluster.register(NodeData(nodeId, hostname))

    logInfo("Waiting for drivers to join")
    initializationBarrier.waitOnBarrier()
  }

  override def reset(cluster: FlareCluster): Unit = {
    val zk = cluster.zk
    val resetBarrier = new DistributedBarrier(zk, "/reset/barrier")
    resetBarrier.setBarrier()

    val resetLeader = new LeaderLatch(zk, "/reset/leader")

    val initializationBarrier = new DistributedBarrier(zk, "/init/barrier")
    initializationBarrier.setBarrier()

    resetLeader.addListener(new LeaderLatchListener() {
      override def isLeader() = {
        logInfo("Waiting for all executors to exit")

        while(!cluster.executors.isEmpty) {
          logInfo(s"Waiting for executors: ${cluster.executors.map(_.executorId).mkString(",")}")
          Thread.sleep(3000)
          cluster.refreshMembers()
        }

        resetBarrier.removeBarrier()
      }
      override def notLeader() = {
        logDebug("Not reset leader, a different node is watching for all executors to exit")
      }
    })

    resetLeader.start()

    logInfo("Waiting on all executors to exit")
    resetBarrier.waitOnBarrier()

    resetLeader.close()

    logInfo("Resetting pool backend")
    poolBackend.reset()

    logInfo("Waiting for drivers")
    initializationBarrier.waitOnBarrier()
  }
}

case class ExecutorClusterProfile(executorId: String, hostname: String) extends FlareClusterProfile with Logging {
  override def start(cluster: FlareCluster): Unit = {
    val initializationBarrier = new DistributedBarrier(cluster.zk, "/init/barrier")

    cluster.register(ExecutorData(executorId, hostname))
    initializationBarrier.waitOnBarrier()
  }
}

case class DriverClusterProfile(hostname: String, port: Int, appId: String, properties: Map[String, String]) extends FlareClusterProfile with Logging {
  override def start(cluster: FlareCluster): Unit = {
    val zk = cluster.zk
    val ser = cluster.serializer.newInstance()

    val resetBarrier = new DistributedBarrier(zk, "/reset/barrier")

    logInfo("Waiting for cleanup on previous app")
    resetBarrier.waitOnBarrier()

    val initializationBarrier = new DistributedBarrier(zk, "/init/barrier")
    val leaderLatch = new LeaderLatch(zk, "/init/leader")

    if (cluster.drivers.isEmpty) {
      logInfo("No connected drivers, setting barrier")
      initializationBarrier.setBarrier()
      leaderLatch.addListener(new LeaderLatchListener() {
        override def isLeader() = {
          logInfo("Initializing app")

          if (zk.checkExists().forPath("/app") != null) {
            zk.delete().deletingChildrenIfNeeded().forPath("/app")
          }

          if (zk.checkExists().forPath("/counter") != null) {
            zk.delete().deletingChildrenIfNeeded().forPath("/counter")
          }

          zk.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/app/id", ser.toBytes(appId))
          zk.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/app/properties", ser.toBytes(properties))

          logInfo("Removing initializing barrier")
          initializationBarrier.removeBarrier()
        }

        override def notLeader() = {
          logInfo("Waiting for leader driver to initialize app")
        }
      })

      leaderLatch.start()
    }

    logInfo("Waiting for initialization")
    initializationBarrier.waitOnBarrier()

    if (leaderLatch.getState() == LeaderLatch.State.STARTED)
      leaderLatch.close()

  }
}