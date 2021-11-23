package com.preprocessing.partitioning

import com.Typedefs.{EitherEdgeRDD, UnweightedEdge, WeightedEdge}
import com.preprocessing.partitioning.PartitioningType.{Hybrid, OneDim, TwoDim}
import com.preprocessing.partitioning.Util.{createPartitionDir, edgeListMatchAndPersist, hybridPartitioningPreprocess, parseArgs, persist, readEdgeList}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

// runMain com.preprocessing.partitioning.Driver --nNodes 986 --nEdges 24929 --inputFilename "src/main/resources/graphs/email-Eu-core/reset/part-00000" --outputDirectoryName "src/main/resources/graphs/email-Eu-core/partitions" --sep " " --partitioner 2 --threshold 100 --numPartitions 4 --partitionBySource "false" --isWeighted "true"

object Driver {

  def main(args: Array[String]): Unit = {

    val appName: String = "edgeList.partitioning.Driver"

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val hadoopConfig = sc.hadoopConfiguration
    hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    val pArgs = parseArgs(args)
    val threshold = pArgs.threshold
    val infile = pArgs.inputFile
    val sep = pArgs.separator
    val partitioner = pArgs.partitioner
    val numPartitions = pArgs.numPartitions
    val outdir = pArgs.outputDirectory
    val partitionBySource: Boolean = pArgs.partitionBySource.toBoolean
    var partitionBySourceDirName = ""
    val isWeighted = pArgs.isWeighted
    var wtStr = ""

    if (partitionBySource) partitionBySourceDirName = "bySrc" else partitionBySourceDirName = "byDest"
    if (isWeighted) wtStr = "Weighted" else wtStr = "Unweighted"

    val partitioningType = PartitioningType(partitioner)
    var execStartString: String = ""
    execStartString += s"${partitioningType.toString} partitioning $wtStr $infile ${partitionBySourceDirName} " +
      s"into $numPartitions partitions " +
      s"with degree threshold = $threshold qualifying a vertex as high degree."
    println(execStartString)
    println("reading edge list...")
    val edgeList = readEdgeList(sc, infile, sep, isWeighted)

    partitioningType match {
      case OneDim =>
        val partitionDir = outdir + s"/1d/$partitionBySourceDirName/"
        println(s"1D Partitioning to ${partitionDir}")
        createPartitionDir(partitionDir, true)
        val partitioner = new OneDimPartitioner(numPartitions, partitionBySource)
        edgeListMatchAndPersist(edgeList, partitioner, partitionDir)

      case TwoDim =>
        val partitionDir = outdir + s"/2d/$partitionBySourceDirName/"
        println(s"2D Partitioning to to ${partitionDir}")
        createPartitionDir(partitionDir, true)
        val partitioner = new TwoDimPartitioner(numPartitions, partitionBySource)
        edgeListMatchAndPersist(edgeList, partitioner, partitionDir)

      case Hybrid =>
        val partitionDir = outdir + s"/hybrid/$partitionBySourceDirName/"
        println(s"Hybrid Partitioning to ${partitionDir}")
        createPartitionDir(partitionDir, true)
        val partitioner = new HybridCutPartitioner(numPartitions, partitionBySource)
        val flaggedEdgeList = hybridPartitioningPreprocess(edgeList, threshold, partitionBySource)

        flaggedEdgeList match {
          case Right(flaggedEdgeList) => // Unweighted, flagged, indexed
            val el = flaggedEdgeList
              .partitionBy(partitioner)
              .map(t => t._1._1)
              .map(r => s"${r._1} ${r._2}")
            persist[String](el, partitionDir, 0)
          case Left(flaggedEdgeList) => // Weighted, flagged, indexed
            val el = flaggedEdgeList
              .partitionBy(partitioner)
              .map(t => t._1._1)
              .map(r => s"${r._1} ${r._2} ${r._3}")
            persist[String](el, partitionDir, 0)
        }
    }
    sc.stop()
  }
}
