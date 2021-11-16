package com.preprocessing.partitioning

//import akka.protobufv3.internal.UInt32Value
//import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator.getObjectSize
import com.preprocessing.partitioning.Util.{createPartitionDir, hybridPartitioningPreprocess, parseArgs, readEdgeList}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.lang.instrument.Instrumentation

// runMain com.preprocessing.partitioning.Driver --nNodes 1005 --nEdges 24929 --inputFilename "src/main/resources/graphs/email-Eu-core/reset/part-00000" --outputDirectoryName "src/main/resources/graphs/email-Eu-core/partitioned" --sep " " --partitioner 3 --threshold 100 --numPartitions 4 --partitionBySource 0

// runMain com.preprocessing.partitioning.Driver --nNodes 8 --nEdges 32 --inputFilename "src/main/resources/graphs/8rmat" --outputDirectoryName "src/main/resources/graphs/8rmat/partitions" --sep " " --partitioner 3 --threshold 100 --numPartitions 4 --partitionBySource 0

object Driver {

  def main(args: Array[String]): Unit = {

    val appName: String = "edgeList.partitioning.Driver"

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val pArgs = parseArgs(args)
    val threshold = pArgs.threshold
    val infile = pArgs.inputFile
    val sep = pArgs.separator
    val partitioner = pArgs.partitioner
    val numPartitions = pArgs.numPartitions
    val outdir = pArgs.outputDirectory
    val partitionBySource = pArgs.partitionBy
    var partitionBySourceDirName = ""

    if (partitionBySource) {
      partitionBySourceDirName = "bySrc"
    } else {
      partitionBySourceDirName = "byDest"
    }

    println("reading edge list...")
    val edgeList = readEdgeList(sc, infile, sep)

    partitioner match {
      case 1 =>
        println("1D Partitioning")
        val partitionDir = outdir + s"/1d/$partitionBySourceDirName/"
        createPartitionDir(partitionDir, true)
        val partitioner = new OneDimPartitioner(numPartitions, partitionBySource)


      case 2 =>
        println("2D Partitioning")
        val partitionDir = outdir + s"/2d/$partitionBySourceDirName/"
        createPartitionDir(partitionDir, true)
        val partitioner = new TwoDimPartitioner(numPartitions, partitionBySource)

      case 3 =>
        println("Hybrid Partitioning")
        val partitionDir = outdir + "/hybrid/"
        val partitioner = new HybridCutPartitioner(numPartitions, partitionBySource)

        try {
          val flaggedEdgeList = hybridPartitioningPreprocess(edgeList, threshold)
          flaggedEdgeList
            .partitionBy(partitioner)
            .mapPartitionsWithIndex {
              (index, itr) => itr.toList.map(x => x + "#" + index).iterator
            }.saveAsTextFile(partitionDir)
        } catch {
          case e: org.apache.hadoop.mapred.FileAlreadyExistsException => println("File already exists, please delete the existing file")
        }
    }
    sc.stop()

//    val inEdgeFile = "src/main/resources/graphs/email-Eu-core/reset/inedge"
//    val degreeMap = "src/main/resources/graphs/email-Eu-core/reset/degreeMap"
    //
    //    inEdgeList.map(e => s"${e._1} ${e._2}").coalesce(1, false).saveAsTextFile(inEdgeFile)
    //    sc.parallelize(inDegrees.toSeq).map(e => s"${e._1} ${e._2}").coalesce(1, false).saveAsTextFile(degreeMap)


//    flaggedEdgeList
//      .partitionBy(new HybridCutPartitioner(numPartitions))
//      .map(el => el._1)
//      .saveAsTextFile(outdir)

  }
}