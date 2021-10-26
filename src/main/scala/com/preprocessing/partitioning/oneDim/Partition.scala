package com.preprocessing.partitioning.oneDim

import scala.collection.mutable.ArrayBuffer

class Partition {
  var id: Int = -1
  var vertices = ArrayBuffer[Vertex]() // main vertices
  var edges = ArrayBuffer[Edge]()
  var adjacencyList = ArrayBuffer[ArrayBuffer[Int]]() // local to this partition
  var mains = ArrayBuffer[Int]()
  var mirrors = ArrayBuffer[Int]()

  def add(v: Vertex): Unit = {
    vertices.addOne(v)
  }

  def add(e: Edge): Unit = {
    edges.addOne(e)
  }
}

object Partition {
  def apply(pid: Int): Partition = {
    val p = new Partition
    p.id = pid
    p
  }
}