package com.preprocessing.partitioning.oneDim

import scala.collection.mutable.ArrayBuffer

class Vertex {
  var id: Int = -1
  var neighbors = ArrayBuffer[Vertex]()
}

object Vertex {
  def apply(vid: Int): Vertex = {
    val v = new Vertex
    v.id = vid
    v
  }
}