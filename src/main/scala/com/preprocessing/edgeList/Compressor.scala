package com.preprocessing.edgeList

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Compressor {
  """
Usage:
runMain com.preprocessing.edgeList.Compressor
--nNodes:         number of vertices in the graph
--nEdges:         number of edges in the graph
--inputFilename:  path to input (uncompressed, unordered) graph
--outputFilename: output path
--sep:            separator used in input edge list (e.g. " ", ",",  "\t")

runMain com.preprocessing.edgeList.Compressor --nNodes 5 --nEdges 7 --inputFilename "src/main/resources/gaps" --outputFilename "src/main/resources/reset" --sep " "

runMain com.preprocessing.edgeList.Compressor --nNodes 1005 --nEdges 25571 --inputFilename "src/main/resources/graphs/email-Eu-core/orig.net" --outputFilename "src/main/resources/graphs/email-Eu-core/reset" --sep " "

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
  val usage =
    """
      |[--nNodes int] [--nEdges int] [--inputFilename str] [--outputFilename str]
      |""".stripMargin

  def main(args: Array[String]): Unit = {

    val appName: String = "edgeList.Compressor.Driver"

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    var nNodes = 0
    var nEdges = 0
    var infile = ""
    var outfile = ""
    var sep = ""

    args.sliding(2, 2).toList.collect {
      case Array("--nNodes", argNNodes: String) => nNodes = argNNodes.toInt
      case Array("--nEdges", argNEdges: String) => nEdges = argNEdges.toInt
      case Array("--inputFilename", argInFile: String) => infile = argInFile
      case Array("--outputFilename", argOutFile: String) => outfile = argOutFile
      case Array("--sep", argSep: String) => sep = argSep
    }

    var argStr = ""
    argStr += s"$nNodes\t"
    argStr += s"$nEdges\t"
    argStr += s"$infile\t"
    argStr += s"$outfile\t"
    println(s"args: $argStr")

    var resetIndex = 0

    // a map between the original vertex id to the vertex id in the compressed, sorted representation
    // TODO; map may contain large amount of vertices; can/should it be distributed?
    // var permutationPairRDD = RDD[(Int, Int)]()?
    val permutationMap = collection.mutable.Map[Int, Int]()

    def readEdgeList(): RDD[(Int, Int)] = {
      val distFile: RDD[String] = sc.textFile(infile)

      val edgeList: RDD[(Int, Int)] = distFile
        .filter(s => !s.contains("#")) // ignore comments
        .map(s => {
          val split = s.split(sep)
          (split(0).toInt, split(1).toInt)
        })

      var nUniqueVertices = 0
//      var uniqueVertexSet = collection.mutable.Set[Int]()
//      edgeList.collect().foreach(edge => {
//        if (!uniqueVertexSet.contains(edge._1)) {
//          uniqueVertexSet.add(edge._1)
//          nUniqueVertices += 1
//        }
//        if (!uniqueVertexSet.contains(edge._2)) {
//          uniqueVertexSet.add(edge._2)
//          nUniqueVertices += 1
//        }
//      })
//      println(s"total unique vertices seen: $nUniqueVertices")
//
//      println("distinct before: ", edgeList.distinct().count())

//      val selfEdges = edgeList.filter(edge => edge._1 == edge._2).collect()
      edgeList
      // for some reason, issues with performing the deduplication at this stage
      //        .filter(edge => edge._1 != edge._2)
      //        .distinct()
    }

    def getOrElseAutoIncrement(permutationMap: collection.mutable.Map[Int, Int], key: Int): Unit = {
      if (!permutationMap.isDefinedAt(key)) {
        permutationMap(key) = resetIndex
        resetIndex += 1
      }
    }

    println("reading edge list...")
    val edgeList = readEdgeList()

    println("sorting edge list...")
    val sortedEdgeList = edgeList.sortBy(e => (e._1, e._2))
    val sorted = sortedEdgeList.collect()

    println("mapping edge list...")
    sorted.foreach(edge => {
      getOrElseAutoIncrement(permutationMap, edge._1)
    })
    sorted.foreach(edge => {
      getOrElseAutoIncrement(permutationMap, edge._2)
    })

    println("remapping edge list...")
    val remapped = edgeList
      .map(edge => {
        (permutationMap(edge._1), permutationMap(edge._2))
      })
      // remove self directed edges and duplicate edges
      .filter(edge => edge._1 != edge._2)
      .distinct()
      .sortBy(edge => (edge._1, edge._2))

    println("ensuring number of edges, number of nodes is consistent")
    val remappedNNodes: Int = permutationMap.size
    val remappedNEdges: Int = remapped.count().toInt

    println(s"Input listed ${nNodes} vertices; After compression: ${remappedNNodes}")
    println(s"Input listed ${nEdges} edges; After compression: ${remappedNEdges}")
    assert(remappedNNodes == nNodes)
    //    assert(remappedNEdges == nEdges)

    println(s"writing compressed edge list to $outfile...")
    try {
      remapped
        .map(e => s"${e._1} ${e._2}")
        .coalesce(1, false) // TODO; partition into multiple files/numPartitions HERE
        .saveAsTextFile(outfile)
    } catch {
      case e: org.apache.hadoop.mapred.FileAlreadyExistsException => println("File already exists, please delete the existing file")

    }

    class VertexID(i: String) {
      val id: String = i
      println(id)
      override def hashCode(): Int = {
        println(id)
        id.split("\\.")(1).toInt
      }
    }

    val vid = new VertexID("9.1")
    println(vid)
    println(vid.hashCode())
    println("stopping spark context...")
    // TODO; Figure out: Cleaner thread interrupted, will stop
    // java.lang.InterruptedException
    // https://stackoverflow.com/a/45301125
    sc.stop()
  }
}
