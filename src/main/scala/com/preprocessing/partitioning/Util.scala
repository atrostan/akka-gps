package com.preprocessing.partitioning

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.nio.file.Files.createDirectory
import java.io.{File, IOException}
import java.nio.file.{Path, Paths}
import org.apache.commons.io.FileUtils.cleanDirectory

object Util {
  /**
   * Parse
   * @param args
   * @return a PartitionerArgs object: a Wrapper for the arguments the partitioner driver expects
   */
  def parseArgs(args: Array[String]): PartitionerArgs = {

    var nNodes = 0
    var nEdges = 0
    var infile = ""
    var outdir = ""
    var sep = ""
    var partitioner = 0 // 1 = 1d, 2 = 2d, 3 = hybrid-cut
    val threshold = 100
    var numPartitions = 0
    var partitionBy = false // 0: "source", 1: "destination"

    // TODO; add an additional argument and functionality to handle weighted graphs
    args.sliding(2, 2).toList.collect {
      case Array("--nNodes", argNNodes: String) => nNodes = argNNodes.toInt
      case Array("--nEdges", argNEdges: String) => nEdges = argNEdges.toInt
      case Array("--graphDir", argInFile: String) => infile = argInFile
      case Array("--outputDirectoryName", argOutDir: String) => outdir = argOutDir
      case Array("--sep", argSep: String) => sep = argSep
      case Array("--partitioner", argPartitioner: String) => partitioner = argPartitioner.toInt
      case Array("--numPartitions", argNumPartitions: String) => numPartitions = argNumPartitions.toInt
      case Array("--partitionBy", argPartitionBy: String) => partitionBy = argPartitionBy.toBoolean
    }
    val partitionerArgs = new PartitionerArgs(
      nNodes,
      nEdges,
      infile,
      outdir,
      sep,
      partitioner,
      threshold,
      numPartitions,
      partitionBy
    )
    partitionerArgs
  }

  /**
   *
   * @param edgeList
   * @param threshold
   * @return
   */
  def hybridPartitioningPreprocess(
                                    edgeList: RDD[(Int, Int)],
                                    threshold: Int
                                  ): RDD[(((Int, Int), Boolean), Long)] = {
    edgeList
      .map(e => (e._2, e._1)) // reverse directions of edges; i.e. convert from out-edge list to in-edge list
      .countByKey() // count number of in-edges per vertex
      .filter(p => p._2 > threshold)
      .map(p => p._1).toSet //
    val inEdgeList = edgeList.map(e => (e._2, e._1))
    val inDegrees: scala.collection.Map[Int, Long] = inEdgeList.countByKey()
    val highInDegreeVertices: Set[Int] = inDegrees.filter(p => p._2 > threshold).map(p => p._1).toSet
    edgeList.map(e => (e, highInDegreeVertices.contains(e._2))).zipWithUniqueId()
  }


  /**
   *
   * @param sc
   * @param infile
   * @param sep
   * @return
   */
  def readEdgeList(sc: SparkContext, infile: String, sep: String): RDD[(Int, Int)] = {
    val distFile: RDD[String] = sc.textFile(infile)

    val edgeList: RDD[(Int, Int)] = distFile
      .filter(s => !s.contains("#")) // ignore comments
      .map(s => {
        val split = s.split(sep)
        (split(0).toInt, split(1).toInt)
      })
    edgeList
  }

  /**
   * Try to create a directory that will store the partitions of a graph.
   * If the directory already exists, optionally erase all contents.
   *
   * @param dirPath
   * @param flush
   */
  def createPartitionDir(dirPath: String, flush: Boolean) = {
    val directoryPath: Path = Paths.get(dirPath)
    try {
      createDirectory(directoryPath)
    } catch {
      case e: IOException =>
        println(s"$dirPath already Exists.")
        if (flush) {
          println(s"Cleaning $dirPath")
          val dirPathFile: File = new File(dirPath)
          cleanDirectory(dirPathFile)
        }
    }
  }

}
























