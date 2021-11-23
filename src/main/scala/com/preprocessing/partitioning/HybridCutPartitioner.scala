package com.preprocessing.partitioning

import org.apache.spark.Partitioner

class HybridCutPartitioner(nPartitions: Int, partitionBySource: Boolean) extends Partitioner {
  val numPartitions = nPartitions


  def getPartitionNum(u: Int, v: Int, vertexIsHighDegree: Boolean): Int = {
    if (partitionBySource) {
      if (vertexIsHighDegree) v % numPartitions
      else u % numPartitions
    } else {
      if (vertexIsHighDegree) u % numPartitions
      else v % numPartitions
    }
  }

  override def getPartition(key: Any): Int = key match {
    case ((u: Int, v: Int), vertexIsHighDegree: Boolean) =>
      getPartitionNum(u, v, vertexIsHighDegree)

    case ((u: Int, v: Int, w: Int), vertexIsHighDegree: Boolean) =>
      getPartitionNum(u, v, vertexIsHighDegree)
  }
}
