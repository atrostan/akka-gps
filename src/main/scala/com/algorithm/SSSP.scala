package com.algorithm

object SSSP extends VertexProgram[Int, Int, Int, Int, Int] {

  override def gather(edgeVal: Int, message: Int): Int = {
    edgeVal + message
  }

  override def sum(a: Int, b: Int): Int = {
    Math.min(a, b)
  }

  override def apply(superStepNumber: Int, thisVertexId: Int, oldVal: Int, total: Option[Int]): Int = {
    if(thisVertexId == 0) {
      0
    } else {
      total match {
        case Some(value) => Math.min(oldVal, value)
        case None => oldVal
      }
    }
  }

  override def scatter(thisVertexId: Int, oldVal: Int, newVal: Int): Option[Int] = {
    if(newVal < oldVal) {
      Some(newVal)
    } else {
      assert(newVal == oldVal, s"Unexpected newVal=${newVal}, oldVal=${oldVal}")
      None
    }
  }

  override def voteToHalt(oldVal: Int, newVal: Int): Boolean = true

  override val defaultActivationStatus: Boolean = true

  override val defaultVertexValue: Int = Integer.MAX_VALUE
}
