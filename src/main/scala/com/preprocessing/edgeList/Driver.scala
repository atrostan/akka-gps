package com.preprocessing.edgeList

import com.Typedefs.{UnweightedEdge, WeightedEdge}
import com.preprocessing.partitioning.Util.{readEdgeList, saveUnweightedRDDAsDF, saveWeightedRDDAsDF}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{BufferedOutputStream, File, PrintWriter}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

object Driver {
  """
Usage:
runMain com.preprocessing.edgeList.Compressor

--inputFilename:  path to input (uncompressed, unordered) graph
--outputFilename: output path
--sep:            separator used in input edge list (e.g. " ", ",",  "\t")

runMain com.preprocessing.edgeList.Driver --inputFilename "src/main/resources/graphs/email-Eu-core/orig.net" --outputFilename "src/main/resources/graphs/email-Eu-core/compressed"  --sep " " --isWeighted "false" --symmetrize "true"

Sort an input edge list by ascending source id. For each source id, the destination ids are also sorted in
ascending order.
Self directed edges are removed.
Multiple edges between the same pair of nodes are removed.

e.g.
Given an unordered edge list with "gaps":
  0 1
  0 0
  0 3
  0 4
  0 2
  3 1
  4 0
  3 3
  3 0

The edgeList.Compressor will produce
  0 1
  0 2
  0 3
  0 4
  3 0
  3 1
  4 0

"""


  /**
   * Save the Number of Nodes (n) and Edges (m) in the compressed representation to a .yml file in the same directory
   * as the input graph so that n and m may be reused in the downstream partitioning and cluster applications.
   * @param infile: the location of the uncompressed graph
   * @param c: Compressor
   */
  def exportYML(infile: String, c: Compressor): Unit = {
    val ymlPath = infile
      .split('/')
      .dropRight(1)
      .mkString("/") + "/stats.yml"

    println(s"Saving nNodes, nEdges to $ymlPath")

    val pw = new PrintWriter(new File(ymlPath))
    pw.write(s"Nodes:\t ${c.nNodes}\n")
    pw.write(s"Edges:\t ${c.nEdges}\n")
    pw.close
  }

  /**
    * Save the Number of Nodes (n) and Edges (m) in the compressed representation to a .yml file in the same directory
    * as the input graph (on HDFS) so that n and m may be reused in the downstream partitioning and cluster applications.
    * @param infile: the location of the uncompressed graph
    * @param sc: SparkContext (needed for hadoop config)
    * @param c: Compressor
    */
  def exportYMLtoHDFS(infile: String, c: Compressor, sc: SparkContext): Unit = {
    val ymlPath = infile
      .split('/')
      .dropRight(1)
      .mkString("/") + "/stats.yml"

    println(s"Saving nNodes, nEdges to $ymlPath")

    val hdfsConf = sc.hadoopConfiguration
    val fileSystem: FileSystem = FileSystem.get(hdfsConf)
    val path = new Path(ymlPath)
    if (fileSystem.exists(path)) fileSystem.delete(path, true)

    val hdfsFileOS: FSDataOutputStream = fileSystem.create(path);
    // create a buffered output stream using the FSDataOutputStream
    val bos = new BufferedOutputStream(hdfsFileOS)

    bos.write(s"Nodes:\t ${c.nNodes}\n".getBytes("UTF-8"))
    bos.write(s"Edges:\t ${c.nEdges}\n".getBytes("UTF-8"))
    bos.close()
  }

  def main(args: Array[String]): Unit = {

    val appName: String = "edgeList.Compressor.Driver"

    val conf = new SparkConf()
      .setAppName(appName)
//      .setMaster("local[*]") // uncomment to run locally
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark: SparkSession = SparkSession.builder.getOrCreate

    val hadoopConfig = sc.hadoopConfiguration
    hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    var infile = ""
    var outfile = ""
    var sep = ""
    var isWeighted: Boolean = false
    // set this flag to symmetrize an input graph for bidirectional vertex programs (e.g. Local Maxima Colouring)
    var symmetrize: Boolean = false

    args.sliding(2, 2).toList.collect {
      case Array("--inputFilename", argInFile: String)   => infile = argInFile
      case Array("--outputFilename", argOutFile: String) => outfile = argOutFile
      case Array("--sep", argSep: String)                => sep = argSep
      case Array("--isWeighted", argWt: String)          => isWeighted = argWt.toBoolean
      case Array("--symmetrize", argSymm: String)        => symmetrize = argSymm.toBoolean
    }

    println("#"*68)
    println("Compression Args:")
    println("#"*68)

    println(s"infile: $infile")
    println(s"outfile: $outfile")
    println(s"sep: $sep")
    println(s"isWeighted: $isWeighted")
    println(s"symmetrize: $symmetrize")

    println("reading edge list...")
    val edgeList = readEdgeList(sc, infile, sep, isWeighted, symmetrize)
    println("compressing...")
    val c = new Compressor(edgeList)

    val compressed = c.compress()
//    if ("infile".contains("hdfs:/")) {
//      exportYMLtoHDFS(infile, c, sc)
//    } else {
//      exportYML(infile, c)
//    }


    /**
      * Check that a valid symmetric graph was created by constructing an adjacency list for each vertex in the graph in
      * both directions, and check that the two adjacency lists are equal
      * @param edgeList a compressed edgelist (may or may not be already symmetric)
      */
    def checkSymmetric(edgeList: RDD[(Int, Int)]) = {
      val outs = edgeList.groupByKey()
      val ins = edgeList.map{ case (src, dest) => (dest, src) }.groupByKey()
      if (outs.subtract(ins).count() > 0) {
        throw new RuntimeException("Input Graph was not symmetrized correctly!")
      } else {
        println("Correctly symmetrized the graph!")
      }
    }

    try {
      compressed match {
        case Left(compressed) => // RDD[WeightedEdge]
          val rdd = compressed
            .sortBy(r => (r._2._1, r._2._2, r._2._3))

          // check that graph was correctly symmetrized
          if (symmetrize) { checkSymmetric(rdd.map{ case (edgeIdx, (u, v, wt)) => (u, v)}) }

          println("Also saving as txt for readability")
          rdd.coalesce(1).saveAsTextFile(outfile + ".net")
          saveWeightedRDDAsDF(rdd, spark, outfile)

        case Right(compressed) => // RDD[UnweightedEdge]
          val rdd = compressed
            .sortBy(r => (r._2._1, r._2._2))

          // check that graph was correctly symmetrized
          if (symmetrize) { checkSymmetric(rdd.map{ case (edgeIdx, (u, v)) => (u, v)}) }

          println("Also saving as txt for readability")
          rdd.coalesce(1).saveAsTextFile(outfile + ".net")
          saveUnweightedRDDAsDF(rdd, spark, outfile)

      }
    } catch {
      case e: org.apache.hadoop.mapred.FileAlreadyExistsException =>
        println("File already exists, please delete the existing file")

    }

    println("stopping spark context...")
    // TODO; Figure out: Cleaner thread interrupted, will stop
    // java.lang.InterruptedException
    // https://stackoverflow.com/a/45301125
    sc.stop()
  }
}
