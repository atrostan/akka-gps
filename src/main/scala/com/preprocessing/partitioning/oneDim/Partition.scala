package com.preprocessing.partitioning.oneDim

import com.graph.Edge

import scala.collection.mutable.ArrayBuffer

class Partition(pid: Int){

  // empty at vertex id i, if no vertex mirror exists in partition
  // otherwise, contains reference to vertex mirror
  // TODO; optimization; a bitSet to indicate existence of mirror in partition
  type MirrorMap = collection.mutable.Map[Int, Mirror]
  object MirrorMap {
    def empty: MirrorMap = collection.mutable.Map.empty
    def apply(ms: (Int, Mirror)*): MirrorMap = collection.mutable.Map(ms:_*)
  }

  val id: Int = pid
  var edges: ArrayBuffer[Edge] = ArrayBuffer[Edge]() // the subgraph in this partition
  var mirrorMap = MirrorMap()

  def add(e: Edge): Unit = {
    edges += e
  }

  override def toString(): String = {
    var s: String = ""
    s += s"Partition $id"
    s
  }
}

object Partition {
  def apply(pid: Int): Partition = {
    val p = new Partition(pid)
    p
  }
}