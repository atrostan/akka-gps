package com.preprocessing.partitioning

object PartitioningType extends Enumeration {
  type PartitioningType = Value
  val OneDim = Value(1, "OneDim")
  val TwoDim = Value(2, "TwoDim")
  val Hybrid = Value(3, "Hybrid")
}