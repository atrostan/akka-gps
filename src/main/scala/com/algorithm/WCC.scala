package com.algorithm

object WCC extends VertexProgram[Int, Int, Int, Int] {

  override val mode: VertexProgram.Mode = VertexProgram.Bidirectional

  override def gather(edgeVal: Int, message: Int): Int = message

  override def sum(a: Int, b: Int): Int = Math.min(a, b)

  override def apply(superStepNumber: Int, thisVertex: VertexInfo, oldVal: Int, total: Option[Int]): Int = {
    if(superStepNumber == 0) thisVertex.id
    else total match {
      case Some(componentId) => Math.min(oldVal, componentId)
      case None              => oldVal
    }
  }

  override def scatter(superStepNumber: Int, thisVertex: VertexInfo, oldVal: Int, newVal: Int): Option[Int] = {
    if(newVal < oldVal) Some(newVal)
    else None
  }

  override def deactivateSelf(superStepNumber: Int, oldVal: Int, newVal: Int): Boolean = true

  override val defaultVertexValue: Int = Integer.MAX_VALUE

  override val defaultActivationStatus: Boolean = true
}
