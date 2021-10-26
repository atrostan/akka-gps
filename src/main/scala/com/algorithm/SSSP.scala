package com.algorithm

object SSSP extends VertexProgram[Int, Int, Int, Int, Int] {

  override def gather(edgeVal: Int, message: Int): Int = {
    edgeVal + message
  }

  override def sum(a: Int, b: Int): Int = {
    Math.min(a, b)
  }

  override def apply(thisVertex: Int, oldVal: Int, total: Option[Int]): Int = {
    if(thisVertex == 0) {
      0
    } else {
      total match {
        case Some(value) => Math.min(oldVal, value)
        case None => oldVal
      }
    }
  }

  override def scatter(oldVal: Int, newVal: Int): Option[Int] = {
    if(newVal < oldVal) {
      Some(newVal)
    } else {
      assert(newVal == oldVal, s"Unexpected newVal=${newVal}, oldVal=${oldVal}")
      None
    }
  }

  override def voteToHalt(oldVal: Int, newVal: Int): Boolean = true
}
