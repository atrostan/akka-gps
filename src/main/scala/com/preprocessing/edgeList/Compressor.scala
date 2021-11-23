package com.preprocessing.edgeList

import com.Typedefs.EitherEdgeRDD
import com.preprocessing.partitioning.Util.persist
import org.apache.spark.rdd.RDD

class Compressor(edgeList: EitherEdgeRDD) {

  // a map between the original vertex id to the vertex id in the compressed, sorted representation
  // TODO; map may contain large amount of vertices; can/should it be distributed?
  // var permutationPairRDD = RDD[(Int, Int)]()?
  val permutationMap = collection.mutable.Map[Int, Int]()
  var resetIndex = 0
  var nNodes: Long = 0
  var nEdges: Long = 0

  def getOrElseAutoIncrement(permutationMap: collection.mutable.Map[Int, Int], key: Int): Unit = {
    if (!permutationMap.isDefinedAt(key)) {
      permutationMap(key) = resetIndex
      resetIndex += 1
    }
  }

  // check if key is in a pair RDD
  def isInPairRDD(key: Int, prdd: RDD[(Int, Long)]): Boolean = {
    prdd.lookup(key) match {
      case Seq()      => false
      case _ +: Seq() => true
      case _ +: _     => true
    }
  }

  /**
   * Given a pair rdd of the form: (Edge Index, (Source ID, dest ID))
   * Compress the corresponding edgelist such that vertex ids are zero based and contiguous
   * @param rdd
   * @return
   */
  def compressRdd(rdd: RDD[(Long, (Int, Int))]): RDD[(Long, (Int, Int))] = {
    // get source ids
    val sources: RDD[Int] = rdd
      .map(row => row._2._1)
      .distinct()
      .sortBy(el => el, ascending = true)

    val sourceMap: RDD[(Int, Long)] = sources
      .zipWithIndex()

    val maxSource: Long = sourceMap.map(_._2).max()
    val unseenDestinations: RDD[(Int, Long)] = rdd
      .map(row => row._2._2)
      .distinct()
      .subtract(sources)
      .sortBy(el => el, ascending = true)
      .zipWithIndex()
      .map(el => (el._1, el._2 + maxSource + 1))

    // vertex isomorphism map
    val vertexMap = sourceMap.union(unseenDestinations)
    nNodes = vertexMap.count()
    println(s"Number of nodes in compressed representation: ${nNodes}")

    // debug
//    persist(vertexMap, "src/main/resources/graphs/email-Eu-core/map", 1)

    val sourcesFirst: RDD[(Int, (Int, Long))] = rdd
      .map(row => (row._2._1, (row._2._2, row._1)))

    // remap the edges of the graph using the vertexmap
    val res = sourcesFirst
      .join(vertexMap)
      .map(r => (r._2._1._1, (r._2._2, r._2._1._2)))
      .join(vertexMap)
      .map(r => (r._2._1._2, (r._2._1._1, r._2._2)))
      .map(r => (r._1, (r._2._1.toInt, r._2._2.toInt)))
      .sortByKey()
    nEdges = res.count()
    println(s"Number of edges in compressed representation: ${nEdges}")
    res
  }

  def compress(): EitherEdgeRDD = {
    edgeList match {
      case Left(edgeList) => // RDD[WeightedEdge]
        // a map between edge index to edge weight
        val weightMap: RDD[(Long, Int)] = edgeList
          .map(row => (row._1, row._2._3))
        val rdd: RDD[(Long, (Int, Int))] = edgeList
          .map(row => (row._1, (row._2._1, row._2._2)))
        val withWeights = compressRdd(rdd)
          .join(weightMap)
          .map(row => (row._1, (row._2._1._1, row._2._1._2, row._2._2)))
          .sortByKey()
        Left(withWeights)
      case Right(edgeList) => // RDD[UnweightedEdge]
        val compressed = compressRdd(edgeList)
        Right(compressed)
    }
  }
}
