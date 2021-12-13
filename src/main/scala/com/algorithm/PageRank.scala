package com.algorithm

// Non-normalized PageRank
// Assumes no sink vertices
class PageRank(iters: Int) extends VertexProgram[Int, Double, Double, Double] {

  require(iters >= 1)

  override val mode: VertexProgram.Mode = VertexProgram.Outwards

  override def gather(edgeVal: Int, message: Double): Double = message

  override def sum(a: Double, b: Double): Double = a + b

  override def apply(superStepNumber: Int, thisVertex: VertexInfo, oldVal: Double, total: Option[Double]): Double = {
    if (superStepNumber == 0) {
      // Do nothing on first superstep except send PR to neighbours
      oldVal
    } else {
      // println(s"StepNum: ${superStepNumber}")
      val sum = total.getOrElse(0.0) // Safe: should have received something from every neighbour
      0.15 + (0.85 * sum)
    }
  }

  override def scatter(superStepNumber: Int, thisVertex: VertexInfo, oldVal: Double, newVal: Double): Option[Double] = {
    if(superStepNumber >= iters || thisVertex.degree == 0) {
      None
    } else {
      Some(newVal / thisVertex.degree)
    }
  }

  override def voteToHalt(superStepNumber: Int, oldVal: Double, newVal: Double): Boolean = {
    superStepNumber >= iters
  }
  
  override val defaultVertexValue: Double = 1.0

  override val defaultActivationStatus: Boolean = true

}
