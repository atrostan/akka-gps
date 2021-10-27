package com.preprocessing.partitioning.oneDim

import scala.collection.mutable.ArrayBuffer

object Driver {
  def main(args: Array[String]): Unit = {

    //    val vertices = ArrayBuffer[Vertex]()
    val edges = ArrayBuffer[Edge]()
    //    val g = ArrayBuffer[ArrayBuffer[Int]]() // Adjacency List
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
    println(png)
    val ma: Array[Main] = png.mainArray
    println("main array")
    for (el <- ma) {
      println(el)
      println("neighbors")
      for (neighbour <- el.neighbors) {
        println(s"\t$neighbour")
      }
      println("mirrors")
      for (mirror <- el.mirrors) {
        println(s"\t$mirror")
      }
    }
    println("mirrors")
    for (p <- png.partitions) {
      println(p)
      for (mirror <- p.mirrorMap) {
        println(mirror)
      }
    }
  }
}
