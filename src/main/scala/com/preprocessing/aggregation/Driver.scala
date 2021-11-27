package com.preprocessing.aggregation

import com.preprocessing.partitioning.Util.{getDegreesByPartition, partitionAssignment, partitionMainsDF, partitionMirrorsDF, readMainPartitionDF, readMirrorPartitionDF, readPartitionsAndJoin, readWorkerPathsFromYaml}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


// driver program to test partition aggregation in preparation for ingestion
// akka
object Driver {

  def main(args: Array[String]): Unit = {


    // local spark config
    val appName: String = "preprocessing.aggregation.Driver"
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark: SparkSession = SparkSession.builder.master("local[*]").getOrCreate

    val hadoopConfig = sc.hadoopConfiguration
    hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    val numPartitions = 4

    val workerPaths = "src/main/resources/paths.yaml"

    // a map between partition ids to location on hdfs of mains, mirrors for that partition
    val partitionMap = readWorkerPathsFromYaml(workerPaths: String)

    val path = "src/main/resources/graphs/8rmat/partitions/hybrid/bySrc"
    val mainsPartitionPath = path + "/mains"
    val mirrorsPartitionPath = path + "/mirrors"
    val sep = " "

    // (partition id, (source, destination, weight))
    val edgeList: RDD[(Int, (Int, Int, Int))] = readPartitionsAndJoin(sc, path, numPartitions, sep)

    val (degrees, outNeighbors, inDegreesPerPartition) = getDegreesByPartition(edgeList)

    val (mains, mirrors) = partitionAssignment(degrees, outNeighbors, inDegreesPerPartition)

    // save to file
    partitionMainsDF(mains, spark, partitionMap)
    partitionMirrorsDF(mirrors, spark, partitionMap)

    // read for testing

    for ((pid, path) <- partitionMap) {
      println(s"Reading partition ${pid} in ${path}")
      val mains = readMainPartitionDF(path+"/mains", spark)
      val mirrors = readMirrorPartitionDF(path+"/mirrors", spark)
      println("mains")
      mains.foreach(m => println(s"\t$m"))
      println("mirrors")
      mirrors.foreach(m => println(s"\t$m"))
    }

    sc.stop()
  }
}
