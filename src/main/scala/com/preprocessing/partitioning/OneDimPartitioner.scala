package com.preprocessing.partitioning

import org.apache.spark.Partitioner

class OneDimPartitioner(nPartitions: Int, partitionBySource: Boolean) extends Partitioner {
  val numPartitions = nPartitions

  override def getPartition(key: Any): Int = key match {
    case (u: Int, v: Int) =>
      if (partitionBySource) u % numPartitions
      else v % numPartitions
  }
}
