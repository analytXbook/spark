package org.apache.spark.deploy.flare

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.spark.metrics.source.Source

/**
  * Created by amar on 7/14/16.
  */
class FlareNodeSource(val node: FlareNode) extends Source {
  override val sourceName = "flare-node"
  override val metricRegistry = new MetricRegistry()

  metricRegistry.register(MetricRegistry.name("executors"), new Gauge[Int] {
    override def getValue: Int = node.executors.size
  })
}