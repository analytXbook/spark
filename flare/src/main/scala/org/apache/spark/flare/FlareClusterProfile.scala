package org.apache.spark.flare

import org.apache.curator.framework.recipes.barriers.DistributedBarrier
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}
import org.apache.spark.{Logging, SparkException}
import org.apache.zookeeper.CreateMode
import org.apache.spark.util.SerializerUtils._

sealed trait FlareClusterProfile {
  def start(cluster: FlareCluster): Unit
  def reset(cluster: FlareCluster): Unit
}

case class NodeClusterProfile(nodeId: String, hostname: String) extends FlareClusterProfile with Logging {
  override def start(cluster: FlareCluster): Unit = {
    val initializationBarrier = new DistributedBarrier(cluster.client, "/init/barrier")
    if (cluster.drivers.isEmpty) {
      logInfo("No connected drivers, setting barrier")
      initializationBarrier.setBarrier()
    }

    cluster.register(NodeData(nodeId, hostname))
    logInfo("Waiting for drivers")
    initializationBarrier.waitOnBarrier()
  }

  override def reset(cluster: FlareCluster): Unit = {
    val initializationBarrier = new DistributedBarrier(cluster.client, "/init/barrier")

    val leaderLatch = new LeaderLatch(cluster.client, "/init/reset-leader")

  }
}

case class ExecutorClusterProfile(executorId: String, hostname: String) extends FlareClusterProfile with Logging {
  override def start(cluster: FlareCluster): Unit = {
    val initializationBarrier = new DistributedBarrier(cluster.client, "/init/barrier")

    cluster.register(ExecutorData(executorId, hostname))
    initializationBarrier.waitOnBarrier()
  }

  override def reset(cluster: FlareCluster): Unit = {
    throw new SparkException("Executor attempted to reset cluster")
  }
}

case class DriverClusterProfile(hostname: String, port: Int, appId: String, properties: Map[String, String]) extends FlareClusterProfile with Logging {
  override def start(cluster: FlareCluster): Unit = {
    val client = cluster.client
    val ser = cluster.serializer.newInstance()

    val initializationBarrier = new DistributedBarrier(client, "/init/barrier")
    val leaderLatch = new LeaderLatch(client, "/init/leader")

    if (cluster.drivers.isEmpty) {
      logInfo("No connected drivers, setting barrier")
      leaderLatch.addListener(new LeaderLatchListener() {
        override def isLeader() = {
          logInfo("Acquired leadership, initializing app")

          client.delete().deletingChildrenIfNeeded().forPath("/counters")
          client.inTransaction()
            .delete().forPath("/init/appId")
            .and()
            .create().withMode(CreateMode.PERSISTENT).forPath("/init/appId", ser.toBytes(appId))
            .and()
            .delete().forPath("/init/properties")
            .and()
            .create().withMode(CreateMode.PERSISTENT).forPath("/init/properties", ser.toBytes(properties))
            .and()
            .commit()

          logInfo("Removing initializing barrier")
          initializationBarrier.removeBarrier()
        }

        override def notLeader() = {
          logInfo("Waiting for leader driver to initialize app")
        }
      })
      initializationBarrier.setBarrier()
      leaderLatch.start()
    }

    logInfo("Waiting for drivers")
    initializationBarrier.waitOnBarrier()

    if (leaderLatch.getState() == LeaderLatch.State.STARTED)
      leaderLatch.close()

    val driverIdCounter = cluster.counter("driverId", -1)
    val driverId = driverIdCounter.incrementAtomic().toInt
    cluster.register(DriverData(driverId, hostname, port))
  }

  override def reset(cluster: FlareCluster): Unit = {
    throw new SparkException("Driver attempted to reset cluster")
  }
}


