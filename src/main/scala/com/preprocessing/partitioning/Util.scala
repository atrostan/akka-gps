package com.preprocessing.partitioning

import com.Typedefs.{EitherEdgeRDD, EitherFlaggedEdgeRDD, UnweightedEdge, WeightedEdge}
import org.apache.commons.io.FileUtils.{cleanDirectory, deleteDirectory}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}

import java.io.File
import java.nio.file.Files.createDirectories
import java.nio.file.{Files, Path, Paths}

object Util {

  /** Parse
    * @param args
    * @return
    *   a PartitionerArgs object: a Wrapper for the arguments the partitioner driver expects
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
    var partitionBySource = "" // 0: "destination", 1: "source"
    var isWeighted = false

    // TODO; add an additional argument and functionality to handle weighted graphs
    args.sliding(2, 2).toList.collect {
      case Array("--nNodes", argNNodes: String)              => nNodes = argNNodes.toInt
      case Array("--nEdges", argNEdges: String)              => nEdges = argNEdges.toInt
      case Array("--inputFilename", argInFile: String)       => infile = argInFile
      case Array("--outputDirectoryName", argOutDir: String) => outdir = argOutDir
      case Array("--sep", argSep: String)                    => sep = argSep
      case Array("--partitioner", argPartitioner: String)    => partitioner = argPartitioner.toInt
      case Array("--numPartitions", argNumPartitions: String) =>
        numPartitions = argNumPartitions.toInt
      case Array("--partitionBySource", argPartitionBy: String) =>
        partitionBySource = argPartitionBy
      case Array("--isWeighted", argWt: String) => isWeighted = argWt.toBoolean
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
      partitionBySource,
      isWeighted
    )
    partitionerArgs
  }

  /** Flag the edges of an weighted/unweighted edgelist based on the degree of either the source or destination vertices. If the degree of the vertex in question is greater than the threshold, tag that vertex (and edge) as high degree.
   * @param edgeList
    * @param threshold
    * @param partitionBySource
    * @return
    */
  def hybridPartitioningPreprocess(
      edgeList: EitherEdgeRDD,
      threshold: Int,
      partitionBySource: Boolean
  ): EitherFlaggedEdgeRDD = {
    edgeList match {
      case Left(edgeList) => // Indexed, Weighted RDD
        val elist = edgeList.map(r => (r._2._1, r._2._2))
        if (partitionBySource) {
          val highDegreeVertices = calcHighDegreeVertices(elist, threshold)
          Left(
            edgeList
              .map(e => (e._2, highDegreeVertices.contains(e._2._1)))
              .zipWithUniqueId()
          )
        } else { // partitionByDestination
          // reverse the directionality of the edges
          val revEdgeList = elist.map(e => (e._2, e._1))
          val highDegreeVertices = calcHighDegreeVertices(revEdgeList, threshold)

          Left(
            edgeList
              .map(e => (e._2, highDegreeVertices.contains(e._2._2)))
              .zipWithUniqueId()
          )
        }

      // TODO; Code duplication here; issue with creating function that can handle both RDD[WeightedEdge] AND RDD[UnweightedEdge]
      case Right(edgeList) => // Indexed, Unweighted RDD
        val elist = edgeList.map(r => (r._2._1, r._2._2))
        if (partitionBySource) {
          val highDegreeVertices = calcHighDegreeVertices(elist, threshold)
          Right(
            edgeList
              .map(e => (e._2, highDegreeVertices.contains(e._2._1)))
              .zipWithUniqueId()
          )
        } else { // partitionByDestination
          // reverse the directionality of the edges
          val revEdgeList = elist.map(e => (e._2, e._1))
          val highDegreeVertices = calcHighDegreeVertices(revEdgeList, threshold)
          Right(
            edgeList
            .map(e => (e._2, highDegreeVertices.contains(e._2._2)))
            .zipWithUniqueId()
          )
        }
    }
  }

  def calcHighDegreeVertices(elist: RDD[(Int, Int)], threshold: Int): Set[Int] = {
    val degrees = elist.countByKey()

    degrees
      .filter(p => p._2 > threshold)
      .map(p => p._1)
      .toSet

  }

  /** Read an weighted / unweighted edgelist file into a PairRDD in either of the two following
    * forms: Weighted: (Edge Index, (Source Id, Dest Id, Weight)) Unweighted: (Edge Index, (Source
    * Id, Dest Id))
    *
    * @param infile
    *   location of the edgelist file
    * @param sep
    *   character separating source and destination vertices
    * @param isWeighted
    *   true, if the graph is weighted; false, otherwise
    * @return
    */
  def readEdgeList(
      sc: SparkContext,
      infile: String,
      sep: String,
      isWeighted: Boolean
  ): EitherEdgeRDD = {
    val strList: RDD[String] = sc.textFile(infile)
    val split = strList
      .filter(s => !s.contains("#"))
      .map(s => s.split(sep))
    if (isWeighted) {
      Left(
        split
          .map(s => ((s(0).toInt, s(1).toInt), s(2).toInt))
          .reduceByKey(
            math.max(_, _)
          ) // if multiple edges between same source and dest, take the maximum weighted edge
          .map(row => (row._1._1, row._1._2, row._2)) // unpack again
          .distinct()
          .filter(e => e._1 != e._2)
          .sortBy(e => (e._1, e._2, e._3))
          .zipWithUniqueId()
          .map(r => (r._2, r._1))
      )
    } else {
      Right(
        split
          .map(s => (s(0).toInt, s(1).toInt))
          .distinct()
          .filter(e => e._1 != e._2)
          .sortBy(e => (e._1, e._2))
          .zipWithUniqueId()
          .map(r => (r._2, r._1))
      )
    }
  }

  /** Try to create a directory that will store the partitions of a graph. If the directory already
    * exists, optionally erase all contents.
    *
    * @param dirPath
    * @param flush
    */
  def createPartitionDir(dirPath: String, flush: Boolean): Unit = {

    val directoryPath: Path = Paths.get(dirPath)
    val partitionsPathStr: String = dirPath.split("/").dropRight(1).mkString("/")
    val partitionsPath: Path = Paths.get(partitionsPathStr)
    if (Files.exists(partitionsPath)) {
      if (Files.exists(directoryPath)) {
        if (flush) {
          println(s"${dirPath} already populated; Removing all files from it.")
          val dirPathFile: File = new File(dirPath)
          cleanDirectory(dirPathFile)
          deleteDirectory(dirPathFile)
        }
      }
    } else {
      createDirectories(partitionsPath)
    }
  }

  def edgeListMatchAndPersist(
      edgeList: EitherEdgeRDD,
      partitioner: Partitioner,
      partitionDir: String
  ) = {
    edgeList match {
      case Left(edgeList) => // RDD[WeightedEdge]
        val el = edgeList
          .map(r => ((r._2._1, r._2._2), r._2._3))
          .partitionBy(partitioner)
          .map(r => (r._1._1, r._1._2, r._2))
          .map(r => s"${r._1} ${r._2} ${r._3}")
        persist[String](el, partitionDir, 0)
      case Right(edgeList) => // RDD[UnweightedEdge]
        val el = edgeList
          .map(r => r._2)
          .zipWithUniqueId()
          .partitionBy(partitioner)
          .map(t => t._1)
          .map(r => s"${r._1} ${r._2}")
        persist[String](el, partitionDir, 0)
    }
  }

  def persist[T](el: RDD[T], path: String, numPartitions: Int): Unit = {
    try {
      if (numPartitions > 0) el.coalesce(numPartitions).saveAsTextFile(path)
      else el.saveAsTextFile(path)
    } catch {
      case e: org.apache.hadoop.mapred.FileAlreadyExistsException =>
        println("File already exists, please delete the existing file")
    }
  }
}
