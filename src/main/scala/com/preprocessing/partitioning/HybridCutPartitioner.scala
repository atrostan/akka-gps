package com.preprocessing.partitioning

import org.apache.spark.Partitioner

class HybridCutPartitioner(nPartitions: Int, partitionBySource: Boolean) extends Partitioner {
  val numPartitions = nPartitions

  override def getPartition(key: Any): Int = key match {
    case ((u: Int, v: Int), vertexIsHighDegree: Boolean) =>
      if (partitionBySource) {
        if (vertexIsHighDegree) v % numPartitions
        else u % numPartitions
      } else {
        if (vertexIsHighDegree) u % numPartitions
        else v % numPartitions
      }

    case ((u: Int, v: Int, w: Int), vertexIsHighDegree: Boolean) =>
      if (partitionBySource) {
        if (vertexIsHighDegree) v % numPartitions
        else u % numPartitions
      } else {
        if (vertexIsHighDegree) u % numPartitions
        else v % numPartitions
      }
  }
}
