package com.preprocessing.partitioning

class PartitionerArgs(
                       nNodes: Int,
                       nEdges: Int,
                       infile: String,
                       outdir: String,
                       sep: String,
                       part: Int,
                       thresh: Int,
                       nPartitions: Int,
                       pb: String,
                       iw: Boolean
                     ) {
  val numNodes = nNodes
  val numEdges = nEdges
  val inputFile = infile
  val outputDirectory = outdir
  val separator = sep
  val partitioner = part
  val threshold = thresh
  val numPartitions = nPartitions
  val partitionBySource = pb
  val isWeighted = iw

  override def toString() : String = {
    var argStr = ""
    argStr += s"$numNodes\t"
    argStr += s"$numEdges\t"
    argStr += s"$inputFile\t"
    argStr += s"$outputDirectory\t"
    argStr += s"$separator\t"
    argStr += s"$partitioner\t"
    argStr += s"$threshold\t"
    argStr += s"$numPartitions\t"
    argStr += s"$partitionBySource\t"
    argStr += s"$isWeighted\t"
    argStr
  }
}
