package com.graph

class Edge(u: Vertex, v: Vertex) {
  val source: Vertex = u
  val dest: Vertex = v
  var id: Int = -1 // needed?

  override def toString(): String = {
    var s: String = ""
    s += s"Edge (${source}, ${dest})"
    s
  }
}

object Edge {
  def apply(u: Vertex, v: Vertex): Edge = {
    val e = new Edge(u, v)
    e
  }
}
