package com.preprocessing.partitioning.oneDim

import com.Typedefs.MirrorMap
import com.graph.Edge

import scala.collection.mutable.ArrayBuffer

class Partition(pid: Int) {

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
