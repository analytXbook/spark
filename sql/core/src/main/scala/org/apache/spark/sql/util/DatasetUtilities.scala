package org.apache.spark.sql.util

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec

class DatasetUtilities[T](ds: Dataset[T]) {

  def rddIdOfDataset: Option[Int] = {
    val plan = ds.queryExecution.sparkPlan
    plan.collect {
      case InMemoryTableScanExec(_, _, relation) =>
        Some(relation.cachedColumnBuffers.id)
      case _ => None
    }.head
  }

}
