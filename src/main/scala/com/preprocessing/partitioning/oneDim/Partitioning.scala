package com.preprocessing.partitioning.oneDim

import scala.collection.mutable.ArrayBuffer

class Partitioning {
  var nPartitions: Int = -1
  var partitions = ArrayBuffer[Partition]()

  def partitionEdges(es: ArrayBuffer[Edge]): Unit = { // TODO annoying; not allowing an overload of `partition`
    for (e <- es) {
      // assign edges to partitions
      this.assign(e)
      // assign vertices to partitions
    }
  }

//  def partitionVertices(vs: ArrayBuffer[Vertex]): Unit = {
//    for (v <- vs) {
//      this.assign(v)
//    }
//  }

  def assign(v: Vertex): Unit = {
    val partitionToAssign: Int = v.id % nPartitions
    this.get(partitionToAssign).vertices.addOne(v)
  }

  def assign(e: Edge): Unit = {
    val partitionToAssign: Int = e.source.id % nPartitions
    this.get(partitionToAssign).edges.addOne(e)
  }

  def isin(v: Vertex, partitionId: Int): Boolean = {
    v.id % nPartitions == partitionId
  }

  def isin(e: Edge, partitionId: Int): Boolean = {
    e.source.id % nPartitions == partitionId
  }

  def isin(v: Vertex, p: Partition): Boolean = {
    v.id % nPartitions == p.id
  }

  def isin(e: Edge, p: Partition): Boolean = {
    e.source.id % nPartitions == p.id
  }

  def get(pid: Int): Partition = {
    partitions(pid)
  }

  // Overriding tostring method
  override def toString(): String = {
    var s: String = ""
    for (p <- partitions) {
      s += s"Partition ${p.id}:\n"
      s += "\tEdges:"
      for (e <- p.edges) {
        s += s"(${e.source.id}, ${e.dest.id}), "
      }
      s += "\n"
      s += "\tMain Vertices:"
      for (v <- p.vertices) {
        s += s"${v.id}"
      }
      s += "\n"
    }
    s
  }

}

object Partitioning {
  def apply(nPartitions: Int): Partitioning = {
    val partitioning = new Partitioning
    partitioning.nPartitions = nPartitions
    for (i <- 0 until partitioning.nPartitions) {
      partitioning.partitions.addOne(Partition(i))
    }
    partitioning
  }
}