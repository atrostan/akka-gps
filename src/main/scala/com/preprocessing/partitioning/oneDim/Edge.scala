package com.preprocessing.partitioning.oneDim

class Edge {
  var id: Int = -1 // needed?
  var source: Vertex = _
  var dest: Vertex = _
}

object Edge {
  def apply(source: Vertex, dest: Vertex): Edge = {
    val e = new Edge
    e.source = source
    e.dest = dest
    e
  }
}