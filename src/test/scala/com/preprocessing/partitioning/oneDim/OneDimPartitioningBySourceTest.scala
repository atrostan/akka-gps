package com.preprocessing.partitioning.oneDim

import org.scalatest._

import scala.collection.mutable.ArrayBuffer

class OneDimPartitioningBySourceTest extends FunSuite with Matchers {
  private def approxEquals(value: Double, other: Double, epsilon: Double) =
    scala.math.abs(value - other) < epsilon

  val edges = ArrayBuffer[Edge]()
  val nPartitions: Int = 4
  val nNodes: Int = 5
  val nEdges: Int = 10

  val v0 = Vertex(0)
  val v1 = Vertex(1)
  val v2 = Vertex(2)
  val v3 = Vertex(3)
  val v4 = Vertex(4)

  val e0 = Edge(v0, v1)
  val e1 = Edge(v0, v2)
  val e2 = Edge(v0, v3)
  val e3 = Edge(v1, v2)
  val e4 = Edge(v2, v0)
  val e5 = Edge(v2, v3)
  val e6 = Edge(v3, v0)
  val e7 = Edge(v3, v1)
  val e8 = Edge(v0, v4)
  val e9 = Edge(v4, v0)

  val es = Seq(e0, e1, e2, e3, e4, e5, e6, e7, e8, e9)
  edges.addAll(es)
  val png = Partitioning(nPartitions, edges, nNodes, nEdges)

  test("Correct assignment of Mains") {
    // an assignment from vertex id to partition id that stores main copy
    val mainMap = png.mainArray.map(m => (m.id, m.partition.id))
    val trueMainMap: Map[Int, Int] = Map(
      0 -> 0,
      1 -> 1,
      2 -> 2,
      3 -> 3,
      4 -> 0
    )
    mainMap == trueMainMap
  }

  test("Correct creation of Mirrors") {
    // a map between each vertex to a list of partition ids it is replicated on
    val replicatedLocations = png.mainArray.map(main => (main.id, main.mirrors.map(mrr => mrr.partition.id).toList))
    val trueReplicatedLocations: Map[Int, List[Int]] = Map(
      (0, List(2, 3)),
      (1, List(0, 3)),
      (2, List(0, 1)),
      (3, List(0, 2)),
      (4, List())
    )
    replicatedLocations == trueReplicatedLocations
  }

  test("Total number of edges in original graph equals sum of number of edges across partitions") {
    png.partitions.map(p => p.edges.size).sum == nEdges
  }

  test("Replication Factor Calculation") {
    approxEquals(png.replicationFactor(), 1.600000023841858, 1e-10)
  }

}
