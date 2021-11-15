package com.graph

import scala.collection.mutable.ArrayBuffer

class Vertex(val vid: Int) {
  val id: Int = vid
  var neighbors = ArrayBuffer[Vertex]()
//  val mainPartition: Partition = get(id % nPartitions)
  override def toString(): String = {
    var s: String = ""
    s += s"Vertex ${id}"
    s
  }

}

object Vertex {
  def apply(vid: Int): Vertex = {
    val v = new Vertex(vid)
    v
  }
}
