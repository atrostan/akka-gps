package com.preprocessing.partitioning

import com.Typedefs._
import org.apache.commons.io.FileUtils.{cleanDirectory, deleteDirectory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{Partitioner, SparkContext}
import org.yaml.snakeyaml.Yaml

import java.io.{File, FileInputStream}
import java.nio.file.Files.createDirectories
import java.nio.file.{Files, Path, Paths}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object Util {

  val weightedSchema = StructType(
    List(
      StructField("edgeId", LongType, false),
      StructField("edge", ArrayType(IntegerType, containsNull = false), false)
    )
  )

  val unweightedSchema = StructType(
    List(
      StructField("edgeId", LongType, false),
      StructField("edge", ArrayType(IntegerType, containsNull = false), false)
    )
  )

  /** Parse
    *
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
    var threshold = 0
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
      case Array("--threshold", argThreshold: String)        => threshold = argThreshold.toInt
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

  /** Flag the edges of an weighted/unweighted edgelist based on the degree of either the source or
    * destination vertices. If the degree of the vertex in question is greater than the threshold,
    * tag that vertex (and edge) as high degree.
    *
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

  def readEdgeListDF(spark: SparkSession, infile: String, isWeighted: Boolean): EitherEdgeRDD = {
    if (isWeighted) {
      val df = spark.read.schema(weightedSchema).parquet(infile)
      val rowRDD = df.rdd
      val edgeList: RDD[(Long, (Int, Int, Int))] = rowRDD.map {
        case Row(eid: Long, edge: Seq[Int]) =>
          (eid, (edge(0), edge(1), edge(2)))
      }
      Left(edgeList)
    } else {
      val df = spark.read.schema(unweightedSchema).parquet(infile)
      val rowRDD = df.rdd
      val edgeList: RDD[(Long, (Int, Int))] = rowRDD.map { case Row(eid: Long, edge: Seq[Int]) =>
        (eid, (edge(0), edge(1)))
      }
      Right(edgeList)
    }

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
      else {
        //        el.foreachPartition() // save to a different node in the cluster
        el.saveAsTextFile(path)
      }
    } catch {
      case e: org.apache.hadoop.mapred.FileAlreadyExistsException =>
        println("File already exists, please delete the existing file")
    }
  }

  def saveAsDF(rdd: RDD[Row], schema: StructType, spark: SparkSession, path: String) = {

    val formats = List(
      "parquet"
      //      "orc",
      //      "avro"
    )
    val df = spark.createDataFrame(rdd, schema)
    df.printSchema()
    for (format <- formats) {
      val formattedPath = path + "." + format
      println(s"Saving DataFrame as ${format} to ${formattedPath}")
      df
        .coalesce(1)
        .write
        .mode("overwrite")
        .format(format)
        .save(formattedPath)
    }
  }

  // TODO: try to have a function that takes in both Weighted and Unweighted edge rdd
  // https://stackoverflow.com/a/21640639

  /** save a weighted rdd it as a spark sql dataframe
    *
    * @param rdd
    *   a pair rdd of weighted edges; Each pair: (Long: global 0-based edge index, V: Weighted or
    *   Unweighted edge)
    */
  def saveWeightedRDDAsDF(rdd: RDD[(Long, WeightedEdge)], spark: SparkSession, path: String) = {

    val rowRDD: RDD[Row] = rdd.map { case (eid: Long, (u: Int, v: Int, w: Int)) =>
      Row(eid, List(u, v, w))
    }
    saveAsDF(rowRDD, weightedSchema, spark, path)
  }

  def saveUnweightedRDDAsDF(rdd: RDD[(Long, UnweightedEdge)], spark: SparkSession, path: String) = {

    val rowRDD: RDD[Row] = rdd.map { case (eid: Long, (u: Int, v: Int)) =>
      Row(eid, List(u, v))
    }
    saveAsDF(rowRDD, unweightedSchema, spark, path)
  }

  def partitionMainsDF(
      mains: RDD[TaggedMainRow],
      spark: SparkSession,
      partitionMap: Map[Int, String]
  ): Unit = {

    val rowRDD = mains.map {
      case (
            (vid: Int, pid: Int),
            mirrors: Set[Int],
            neighs: List[(Int, Int, Int)],
            partitionInDegree: Int
          ) =>
        Row(
          vid,
          pid,
          mirrors.toList,
          neighs.flatten { case (dest, wt, tag) => List(dest, wt, tag) },
          partitionInDegree
        )
    }
    val df = spark.createDataFrame(rowRDD, mainRowSchema)
    for ((pid, path) <- partitionMap) {
      println(s"writing mains to ${path}/mains")
      val partition = df.filter(s"partitionId == ${pid}")
      partition.write.mode("overwrite").parquet(path + "/mains")
    }
  }

  def partitionMirrorsDF(
      mirrors: RDD[TaggedMirrorRow],
      spark: SparkSession,
      partitionMap: Map[Int, String]
  ): Unit = {
    val rowRDD = mirrors.map {
      case (
            (vid: Int, pid: Int),
            mainPid: Int,
            neighs: List[(Int, Int, Int)],
            partitionInDegree: Int
          ) =>
        Row(
          vid,
          pid,
          mainPid,
          neighs.flatten { case (dest, wt, tag) => List(dest, wt, tag) },
          partitionInDegree
        )
    }
    val df = spark.createDataFrame(rowRDD, mirrorRowSchema)
    for ((pid, path) <- partitionMap) {
      println(s"writing mirrors to ${path}/mirrors")
      val partition = df.filter(s"partitionId == ${pid}")
      partition.write.mode("overwrite").parquet(path + "/mirrors")
    }
  }

  val mainRowSchema = StructType(
    List(
      StructField("vertexId", IntegerType, false),
      StructField("partitionId", IntegerType, false),
      StructField("partitionsWithMirrors", ArrayType(IntegerType, containsNull = false), false),
      StructField("outgoingEdges", ArrayType(IntegerType, containsNull = false), false),
      StructField("partitionInDegree", IntegerType, false)
    )
  )

  val mirrorRowSchema = StructType(
    List(
      StructField("vertexId", IntegerType, false),
      StructField("partitionId", IntegerType, false),
      StructField("mainPartitionId", IntegerType, false),
      StructField("outgoingEdges", ArrayType(IntegerType, containsNull = false), false),
      StructField("partitionInDegree", IntegerType, false)
    )
  )

  def pairs[A](as: List[A]) = as
    .grouped(2)
    .map {
      case List(x, y) => (x, y)
      case _          => sys.error("uneven size")
    }
    .toList

  def triples[A](as: List[A]) = as
    .grouped(3)
    .map {
      case List(a, b, c) => (a, b, c)
      case _             => sys.error("uneven size")
    }
    .toList

  def readMainPartitionDF(
      path: String,
      spark: SparkSession
  ): RDD[(Int, Set[Int], List[(Int, Int, Int)], Int)] = {
    println(s"reading mains from ${path}")

    val partitionedMainRowSchema = StructType(
      List(
        StructField("vertexId", IntegerType, false),
        StructField("partitionsWithMirrors", ArrayType(IntegerType, containsNull = false), false),
        StructField("outgoingEdges", ArrayType(IntegerType, containsNull = false), false),
        StructField("partitionInDegree", IntegerType, false)
      )
    )

    val rowRDD = spark.read.schema(partitionedMainRowSchema).parquet(path).rdd
    rowRDD.map {
      case Row(
            vid: Int,
            pidsWithMirrors: mutable.WrappedArray[Int],
            neighs: mutable.WrappedArray[Int],
            partitionInDegree: Int
          ) =>
        (vid, pidsWithMirrors.toSet, triples(neighs.toList), partitionInDegree)
    }
  }

  def readMirrorPartitionDF(
      path: String,
      spark: SparkSession
  ): RDD[(Int, Int, List[(Int, Int, Int)], Int)] = {
    println(s"reading mirrors from ${path}")

    val partitionedMirrorRowSchema = StructType(
      List(
        StructField("vertexId", IntegerType, false),
        StructField("mainPartitionId", IntegerType, false),
        StructField("outgoingEdges", ArrayType(IntegerType, containsNull = false), false),
        StructField("partitionInDegree", IntegerType, false)
      )
    )

    val rowRDD = spark.read.schema(partitionedMirrorRowSchema).parquet(path).rdd

    rowRDD map {
      case Row(
            vid: Int,
            mainPartitionId: Int,
            neighs: mutable.WrappedArray[Int],
            partitionInDegree: Int
          ) =>
        (vid, mainPartitionId, triples(neighs.toList), partitionInDegree)
    }
  }

  /** Use pattern matching to resolve Some and None for the outgoing edges and indegree in a row
    * that represents a mirror vertex Some(outgoing edges) => List(outgoing edges) None(outgoing
    * edges) => List() Some(partitionInDegree) => partitionInDegree None(partitionInDegree) => 0
    *
    * @param row
    * @return
    *   MirrorRow
    */
  def cleanupMirrorRow(
      mirrorRow: ((Int, Int), ((Int, Option[Iterable[(Int, Int)]]), Option[Int]))
  ): MirrorRow = {
    val vid = mirrorRow._1._1
    val pid = mirrorRow._1._2
    val mainPid = mirrorRow._2._1._1
    val outEdges = mirrorRow._2._1._2
    val partitionInDegree = mirrorRow._2._2
    (outEdges, partitionInDegree) match {
      case (Some(neighs: List[(Int, Int)]), Some(d: Int)) => ((vid, pid), mainPid, neighs, d)
      case (Some(neighs: List[(Int, Int)]), None)         => ((vid, pid), mainPid, neighs, 0)
      case (None, Some(d: Int))                           => ((vid, pid), mainPid, List(), d)
      case (None, None)                                   => ((vid, pid), mainPid, List(), 0)
    }
  }

  /** Use pattern matching to resolve Some and None for the outgoing edges and indegree in a row
    * that represents a main vertex Some(outgoing edges) => List(outgoing edges) None(outgoing
    * edges) => List() Some(partitionInDegree) => partitionInDegree None(partitionInDegree) => 0
    *
    * @param row
    * @return
    *   MainRow
    */
  def cleanupMainRow(
      mainRow: ((Int, Int), ((Set[_ <: Int], Option[Iterable[(Int, Int)]]), Option[Int]))
  ): MainRow = {
    val vid = mainRow._1._1
    val pid = mainRow._1._2
    val mirrorPids = mainRow._2._1._1
    val outEdges = mainRow._2._1._2
    val partitionInDegree = mainRow._2._2
    // TODO; intellij says toInt redundant, but can't avoid
    val mirrorSet: Set[Int] = mirrorPids.map(_.toInt)
    (outEdges, partitionInDegree) match {
      case (Some(neighs: List[(Int, Int)]), Some(d: Int)) => ((vid, pid), mirrorSet, neighs, d)
      case (Some(neighs: List[(Int, Int)]), None)         => ((vid, pid), mirrorSet, neighs, 0)
      case (None, Some(d: Int))                           => ((vid, pid), mirrorSet, List(), d)
      case (None, None)                                   => ((vid, pid), mirrorSet, List(), 0)
    }
  }

  def getRandomElement[A](seq: Seq[A], random: Random): A =
    seq(random.nextInt(seq.length))

  def mainAssignmentToPartition(partitionsWithOutNeighbors: Iterable[Int]) = {
    partitionsWithOutNeighbors.toSeq match {
      // vertex is present in only one partition
      case Seq(pid) => pid
      // vertex is present in multiple partitions, assign it to a random one
      // TODO: is there a smarter way to assign main to partitions? some heuristic?
      case xs =>
        val random = new Random
        getRandomElement(xs, random)
    }
  }

  def mirrorAssignmentToPartition(
      mainPartitionId: Int,
      partitionsWithOutNeighbors: Iterable[Int],
      partitionsWithInNeighbors: Iterable[Int]
  ) = {
    val partitionsWithNeighbours =
      partitionsWithOutNeighbors.toSet union partitionsWithInNeighbors.toSet
    partitionsWithNeighbours match {
      case xs =>
        xs.filter(pid => pid != mainPartitionId)
    }
  }

  def mainMirrorAssignment(
      vid: Int,
      psWithOutNeighbors: Option[Iterable[Int]],
      psWithInNeighbors: Option[Iterable[Int]]
  ) = {
    (psWithOutNeighbors, psWithInNeighbors) match {
      // @formatter:off
      // vertex has outNeighbors in 1 or more partitions and inNeighbors in 1 or more partitions
      case (Some(psWithOuts: Iterable[Int]), Some(psWithIns: Iterable[Int])) =>
        val mainPid = mainAssignmentToPartition(psWithOuts) // assign the vertex as a main at random to one of them
        val mirrorPids = mirrorAssignmentToPartition(mainPid, psWithOuts, psWithIns)
        (vid, mainPid, mirrorPids)

      case (Some(psWithOuts: Iterable[Int]), None) => // vertex only has outNeighbors in 1 or more partitions
        val mainPid = mainAssignmentToPartition(psWithOuts) // assign the vertex as a main at random to one of them
        (vid, mainPid, psWithOuts.toSet - mainPid)
      // @formatter:on

      // vertex has no outNeighbors in any partition
      // vertex _must_ have some inNeighbors in another partition
      // (otherwise it would not be present in the compressed graph)
      // vertex will be assigned as main to one of the partitions in which it has inNeighbors
      case (None, Some(psWithIns: Iterable[Int])) =>
        // pick one partition at random to be the main partition
        val random = new Random
        val mainPid = getRandomElement(psWithIns.toSeq, random)
        (vid, mainPid, psWithIns.toSet - mainPid)
      // vertex has no inNeighbors or outNeighbors in any partition. why is it in the graph
      case (None, None) =>
        throw new Exception("Vertex with 0 in-degree and 0 out-degree present in the graph!!")
        (-1, -1, Set())

    }
  }

  /** Read all partitions and join them into a single rdd of weighted edges This global knowledge of
    * the (partitioned) edgelist is needed for main/mirror assignment TODO: partitionDir maybe
    * multiple separate nodes on hdfs
    *
    * @param partitionDir
    *   directory that stores all partitions
    * @return
    */
  def readPartitionsAndJoin(
      sc: SparkContext,
      partitionDir: String,
      numPartitions: Int,
      sep: String
  ): RDD[(Int, (Int, Int, Int))] = {
    val rdds = ArrayBuffer[RDD[(Int, (Int, Int, Int))]]()
    for (pid <- 0 until numPartitions) {
      val partitionPath = partitionDir + "/part-%05d".format(pid)
      // add partition id as key
      // add 1 as an edge weight to unweighted rdds so that all rdds are of same type
      rdds.append(
        sc
          .textFile(partitionPath)
          .map(s =>
            s.split(sep) match {
              case Array(u, v, w) => (pid, (u.toInt, v.toInt, w.toInt))
              case Array(u, v)    => (pid, (u.toInt, v.toInt, 1))
            }
          )
      )
    }
    rdds.reduce(_.union(_))
  }

  /** Given a partition edgelist, calculate:
    *   - outneighbours per partition
    *   - indegree per partition
    *   - outdegree per partition
    *
    * @param rdd
    * @return
    */
  def getDegreesByPartition(rdd: RDD[(Int, (Int, Int, Int))]) = {
    val outNeighbors = rdd
      .groupBy(t => (t._1, t._2._1))
      .map(t =>
        (
          (t._1._2, t._1._1),
          t._2.map(t =>
            t match {
              case (pid: Int, (src: Int, dest: Int, wt: Int)) =>
                (dest, wt)
            }
          )
        )
      )

    val outDegrees = rdd
      .groupBy(t => (t._1, t._2._1))
      .map(t => (t._1, t._2))

    val inDegreesPerPartition = rdd
      .groupBy(t => (t._1, t._2._2))
      .map(t => ((t._1._2, t._1._1), t._2.size))

    val inDegrees = rdd
      .groupBy(t => (t._1, t._2._2))
      .map(t => (t._1, t._2))

    // reorder the elements of the edgelist so that the source vertex is the key
    val allOutDegrees = outDegrees.map(t => (t._1._2, t._1._1)).groupByKey()
    val allInDegrees = inDegrees.map(t => (t._1._2, t._1._1)).groupByKey()

    val degrees = allOutDegrees.fullOuterJoin(allInDegrees)

    (degrees, outNeighbors, inDegreesPerPartition)
  }

  /** Given the per-partition out degree and in degree of every node in the graph, assign each node
    * to one partition as a main, and (possibly) multiple partitions as a mirror
    *
    * @param degrees
    */
  def partitionAssignment(
      degrees: RDD[(Int, (Option[Iterable[Int]], Option[Iterable[Int]]))],
      outNeighbors: RDD[((Int, Int), Iterable[(Int, Int)])],
      inDegreesPerPartition: RDD[((Int, Int), Int)]
  ) = {
    val vertexMainAndMirrors: RDD[((Int, Int), Set[_ <: Int])] = degrees
      .map {
        case (
              vid: Int,
              (
                psWithOutNeighbors: Option[Iterable[Int]],
                psWithInNeighbors: Option[Iterable[Int]]
              )
            ) =>
          mainMirrorAssignment(vid, psWithOutNeighbors, psWithInNeighbors)
      }
      .map(t => ((t._1, t._2), t._3))



    vertexMainAndMirrors
      .cache() // cache the mains in memory so that their correct location can be propagated to their mirrors
    val mirrors = vertexMainAndMirrors.flatMap { case ((vid: Int, _), mirrorPids: Set[Int]) =>
      mirrorPids.map(mirrorPid => (vid, mirrorPid))
    }

    // add (partition-local) outgoing neighbours and partition in degree to mains
    val mains = vertexMainAndMirrors
      .leftOuterJoin(outNeighbors)
      .leftOuterJoin(inDegreesPerPartition)
      .map(row => cleanupMainRow(row))

    val mainPids = vertexMainAndMirrors.map { case ((vid: Int, pid: Int), _: Set[Any]) =>
      (vid, pid)
    }
    val mirrorsWithMainPids = mirrors
      // add the main partition id to each mirror vertex (mirrors should know the location of their main)
      .join(mainPids)
      .map { case (vid: Int, (pid: Int, mainPid: Int)) => ((vid, pid), mainPid) }
      .leftOuterJoin(outNeighbors)
      .leftOuterJoin(inDegreesPerPartition)
      .map(row => cleanupMirrorRow(row))

    val nInEdges =
      mirrorsWithMainPids.map(t => t._4).reduce(_ + _) + mains.map(t => t._4).reduce(_ + _)
    val nOutEdges = mirrorsWithMainPids.map(t => t._3.size).reduce(_ + _) +
      mains.map(t => t._3.size).reduce(_ + _)
    println("nInEdges: ", nInEdges)
    println("nOutEdges: ", nOutEdges)
    // TODO: can calculate replication factor here

    (mains, mirrorsWithMainPids)
  }

  def readWorkerPathsFromYaml(workerPaths: String): Map[Int, String] = {
    val config = new FileInputStream(new File(workerPaths))
    val yaml = new Yaml()
    val partitions = mapAsScalaMap(
      yaml
        .load(config)
        .asInstanceOf[java.util.Map[String, Any]]
    )("workers")
      .asInstanceOf[java.util.ArrayList[String]]
    val itr = partitions.listIterator()
    var pid = 0
    val pMap = collection.mutable.Map[Int, String]()
    while (itr.hasNext) {
      pMap(pid) = itr.next()
      pid += 1
    }
    pMap.toMap
  }

  /** Tag each edge in the graph using the following definitions: Each edge is assigned to a partition
    * in the partitioning step. Also, the main assignment step assigned each vertex as a main
    * vertex to some partition. In the following, an edge is between source id -> destination id:
    * if an edge is between main -> main, label that edge as 0
    * if an edge is between main -> mirror, label that edge as 1
    * if an edge is between mirror -> main, label that edge as 2
    * if an edge is between mirror -> mirror, label that edge as 3
    * @param mains
    * @param edgeList
    * @return
    */
  def tagEdges(
      mains: RDD[MainRow],
      edgeList: RDD[(Int, (Int, Int, Int))]
  ): RDD[((Int, Int, Int), Int)] = {
    val mainsWithPartitions = mains.map { case ((vid: Int, pid: Int), _, _, _) => (vid, pid) }
    // for each edge, get the main partition of its source
    val srcPartitions = edgeList
      .map { case (pid: Int, (src: Int, dest: Int, wt: Int)) =>
        (src, (dest, wt, pid))
      }
      .join(mainsWithPartitions)

    // reverse each edge to get the main partition of its destination
    val destPartitions = srcPartitions
      .map { case (src: Int, ((dest: Int, wt: Int, edgePid: Int), srcMainPid: Int)) =>
        (dest, (src, wt, edgePid, srcMainPid))
      }
      .join(mainsWithPartitions)
      .map { case (dest, ((src, wt, edgePid, srcMainPid), destMainPid)) =>
        (src, dest, wt, edgePid, srcMainPid, destMainPid)
      }

    destPartitions.map { case (src, dest, wt, edgePid, srcMainPid, destMainPid) =>
      if ((srcMainPid == edgePid) && (destMainPid == edgePid)) { // main -> main
        ((src, dest, wt), 0)
      } else if ((srcMainPid == edgePid) && (destMainPid != edgePid)) { // main -> mirror
        ((src, dest, wt), 1)
      } else if ((srcMainPid != edgePid) && (destMainPid == edgePid)) { // mirror -> main
        ((src, dest, wt), 2)
      } else { // mirror -> mirror
        ((src, dest, wt), 3)
      }
    }
  }

  def tagMirrors(
      mirrors: RDD[MirrorRow],
      taggedEdges: RDD[((Int, Int, Int), Int)]
  ): RDD[TaggedMirrorRow] = {
    val taggedMirrors = mirrors
      .flatMap { case ((vid, pid), mainPid, outs, partitionInDegree) =>
        // include original mirror key (vid, pid) to rejoin the original mirror rdd once edges have been tagged
        for ((dest, wt) <- outs) yield ((vid, dest, wt), (vid, pid))
      }
      .leftOuterJoin(taggedEdges)
      .map { case ((u, v, w), ((vid, pid), tag)) =>
        tag match {
          case Some(t: Int) => ((vid, pid), (v, w, t))
          case None         => sys.error("untagged edge")
        }
      }
      .groupByKey()

    // filter out untagged neighbours and join tagged neighbours
    mirrors
      .map { case ((vid, pid), mainPid, neighs, partitionInDegree) =>
        ((vid, pid), (mainPid, partitionInDegree))
      }
      .leftOuterJoin(taggedMirrors)
      // resolve some/none neighbours for mirrors
      .map { case ((vid, pid), ((mainPid, partitionInDegree), taggedNeighs)) =>
        taggedNeighs match {
          // todo; should the outgoing neighbour list be sorted?
          case Some(neighs: Seq[(Int, Int, Int)]) =>
            ((vid, pid), mainPid, neighs.toList, partitionInDegree)
          case None => ((vid, pid), mainPid, List(), partitionInDegree)
        }
      }
  }
  def tagMains(
      mains: RDD[MainRow],
      taggedEdges: RDD[((Int, Int, Int), Int)]
  ): RDD[TaggedMainRow] = {
    val taggedMains = mains
      .flatMap { case ((vid, pid), mirrorPids, outs, partitionInDegree) =>
        // include original mirror key (vid, pid) to rejoin the original mirror rdd once edges have been tagged
        for ((dest, wt) <- outs) yield ((vid, dest, wt), (vid, pid))
      }
      .leftOuterJoin(taggedEdges)
      .map { case ((u, v, w), ((vid, pid), tag)) =>
        tag match {
          case Some(t: Int) => ((vid, pid), (v, w, t))
          case None         => sys.error("untagged edge")
        }
      }
      .groupByKey()

    // filter out untagged neighbours and join tagged neighbours
    mains
      .map { case ((vid, pid), mirrorPids, neighs, partitionInDegree) =>
        ((vid, pid), (mirrorPids.asInstanceOf[Set[Int]], partitionInDegree))
      }
      .leftOuterJoin(taggedMains)
      // resolve some/none neighbours for mains
      .map { case ((vid, pid), ((mainPid, partitionInDegree), taggedNeighs)) =>
        taggedNeighs match {
          // todo; should the outgoing neighbour list be sorted?
          case Some(neighs: Seq[(Int, Int, Int)]) =>
            ((vid, pid), mainPid, neighs.toList, partitionInDegree)
          case None => ((vid, pid), mainPid, List(), partitionInDegree)
        }
      }
  }

}
