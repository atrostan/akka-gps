package com.preprocessing.aggregation

import com.preprocessing.aggregation.HDFSUtil.getHDFSfs
import com.preprocessing.aggregation.Serialization.{Main, Mirror, readMainTextFile, readMirrorTextFile, readObjectArray}
import com.preprocessing.partitioning.Util._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, PrintWriter}
import java.nio.file.Files.createDirectories
import java.nio.file.{Path, Paths}

// driver program to test partition aggregation in preparation for ingestion
// akka
object Driver {

  def parseArgs(args: Array[String]) = {
    // folder that contains partitioned graph;
    // expected partition file names: part-%05d (0 based)
    var partitionFolder = ""
    var numPartitions = -1 // number of partitions
    var sep = "" // edge list separator
    var hadoopConfDir = "" // needed to configure hdfs fs to save main, mirror arrays

    args.sliding(2, 2).toList.collect {
      case Array("--partitionFolder", argPartitionFolder: String) =>
        partitionFolder = argPartitionFolder
      case Array("--numPartitions", argNumPartitions: String) =>
        numPartitions = argNumPartitions.toInt
      case Array("--sep", argSep: String)                 => sep = argSep
      case Array("--hadoopConfDir", argHCD: String) => hadoopConfDir = argHCD
    }
    (partitionFolder, numPartitions, sep, hadoopConfDir)
  }
  // runMain com.preprocessing.aggregation.Driver --partitionFolder "src/main/resources/graphs/symmRmat/partitions/hybrid/bySrc" --numPartitions 4 --sep " " --hadoopConfDir "/home/atrostan/Workspace/repos/hadoop/hadoop-3.3.1/etc/hadoop"



  def main(args: Array[String]): Unit = {

    val appName: String = "preprocessing.aggregation.Driver"
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark: SparkSession = SparkSession.builder.getOrCreate

    val hadoopConfig = sc.hadoopConfiguration
    hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    val (partitionFolder, numPartitions, sep, hadoopConfDir) = parseArgs(args)

    // get hdfs filesystem
    val fs = getHDFSfs(hadoopConfDir)
    println(fs)
    // a map between partition ids to location on hdfs of mains, mirrors for that partition
    val partitionMap = (0 until numPartitions)
      .map(i => (i, partitionFolder + s"/p$i")).toMap

    // create mains, mirrors partition dirs if they don't exist
    for ((_, path) <- partitionMap) { createDirectories(Paths.get(path)) }

    // (partition id, (source, destination, weight))
    val edgeList: RDD[(Int, (Int, Int, Int))] = readPartitionsAndJoin(sc, partitionFolder, numPartitions, sep)

    val (degrees, outNeighbors, inDegreesPerPartition) = getDegreesByPartition(edgeList)

    val (mains, mirrors) = partitionAssignment(degrees, outNeighbors, inDegreesPerPartition)

    println("edges")
    edgeList.collect().sortBy(t => (t._1, t._2)).foreach(println)
    println("mains")
    mains.collect().sortBy(t => (t._1)).foreach(println)

    // find out the identity of outgoing neighbours
    val taggedEdges = tagEdges(mains, edgeList)
    val taggedMains = tagMains(mains, taggedEdges)
    val taggedMirrors = tagMirrors(mirrors, taggedEdges)

    // save to file
    partitionMainsDF(taggedMains, spark, partitionMap, fs)
    partitionMirrorsDF(taggedMirrors, spark, partitionMap, fs)
    // read for debug
    println("#"*68)
    println("DEBUG OUTPUT")
    println("#"*68)
    for ((pid, path) <- partitionMap) {
      println(s"Reading partition ${pid} in ${path}")
//      val hdfsMainPath = s"hdfs:///graphs/symmRmat/partitions/1d/bySrc/p${pid}/mains/part-00000"
//      val hdfsMirrorPath = s"hdfs:///graphs/symmRmat/partitions/1d/bySrc/p${pid}/mirrors/part-00000"
      val hdfsMainPath = path + "/mains/part-00000"
      val hdfsMirrorPath = path + "/mirrors/part-00000"
      println(hdfsMainPath)
      val mns = readMainTextFile(hdfsMainPath,fs)
      println("mains")
      mns.foreach(println)
      val mrs = readMirrorTextFile(hdfsMirrorPath,fs)
      println("mirrors")
      mrs.foreach(println)
//      val mains = readMainPartitionDF(path+"/mains", spark)
//      val mirrors = readMirrorPartitionDF(path+"/mirrors", spark)
//      val mains = readObjectArray[Main](path+"/mains.ser")
//      mains match {
//        case ms: Array[Main] =>
//          println("mains")
//          ms.foreach(m => println(s"\t$m"))
//      }
//      val mirrors = readObjectArray[Mirror](path+"/mirrors.ser")
//      mirrors match {
//        case ms: Array[Mirror] =>
//          println("mirrors")
//          ms.foreach(m => println(s"\t$m"))
//      }
    }

    sc.stop()
  }
}
