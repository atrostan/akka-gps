package com.preprocessing.partitioning

import org.apache.spark.Partitioner

class HybridCutPartitioner(nPartitions: Int, partitionBySource: Boolean) extends Partitioner {
  val numPartitions = nPartitions

  override def getPartition(key: Any): Int = key match {
    case ((u: Int, v: Int), vertexIsHighDegree: Boolean) =>
      if (partitionBySource) {
        val uIsHighDegree = vertexIsHighDegree
        if (uIsHighDegree) v % numPartitions
        else u % numPartitions
      } else {
        val vIsHighDegree = vertexIsHighDegree
        if (vIsHighDegree) u % numPartitions
        else v % numPartitions
      }

  }
}
