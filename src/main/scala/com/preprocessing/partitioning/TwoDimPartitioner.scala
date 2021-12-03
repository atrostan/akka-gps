package com.preprocessing.partitioning

import org.apache.spark.Partitioner

class TwoDimPartitioner(nPartitions: Int, partitionBySourceFlag: Boolean) extends Partitioner {
  val numPartitions = nPartitions
  val partitioningMatrixSideLength: Int = math.sqrt(numPartitions).toInt
  val partitionBySource = partitionBySourceFlag
  override def getPartition(key: Any): Int = key match {
    case (x: Int, y: Int) =>
      val sx: Int = x % partitioningMatrixSideLength
      val sy: Int = y % partitioningMatrixSideLength
      if (partitionBySource) sx * partitioningMatrixSideLength + sy
      else sx + partitioningMatrixSideLength * sy
  }
}
