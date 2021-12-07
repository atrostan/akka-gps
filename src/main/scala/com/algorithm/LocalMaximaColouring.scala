package com.algorithm

case class Colour(num: Int) {
  require(num >= -1)
}
object Colour {
  val Blank = Colour(-1)
}

object LocalMaximalColouring extends VertexProgram[Int, Int, Int, Colour] {
  
  // Map messages into accumulator values
  override def gather(edgeVal: Int, message: Int): Int = message

  // Combine accumulator values
  override def sum(a: Int, b: Int): Int = Math.max(a, b)

  // Compute new local state for this vertex based on accumulated sum
  override def apply(superStepNumber: Int, thisVertex: VertexInfo, oldVal: Colour, total: Option[Int]): Colour = {
    // At start, always colour yourself blank
    if (superStepNumber == 0) Colour.Blank
    // If you have a colour, keep it
    else if (oldVal != Colour.Blank) oldVal
    // If you have no neighbours, or you are bigger than the largest ID, colour yourself
    else if (total == None || total.get < thisVertex.id) Colour(superStepNumber - 1)
    // Otherwise, stay blank
    else Colour.Blank
  }

  // Optionally generate a message to send to neighbours
  override def scatter(superStepNumber: Int, thisVertex: VertexInfo, oldVal: Colour, newVal: Colour): Option[Int] = {
    if (newVal == Colour.Blank) Some(thisVertex.id)
    else None
  }

  // Whether to deactivate this vertex at the end of this superstep
  override def deactivateSelf(superStepNumber: Int, oldVal: Colour, newVal: Colour): Boolean = (newVal != Colour.Blank)

  // Initial starting state
  override val defaultVertexValue: Colour = Colour.Blank

  // Initial starting activation status
  override val defaultActivationStatus: Boolean = true

  // Whether messages are sent along out-edges, in-edges, or both
  override val mode = VertexProgram.Bidirectional
}

// object LocalMaximaColouring extends LocalMaximalColouringAbstractMode {
//   override val mode = VertexProgram.Outwards
// }

// object LocalMaximaColouringBidirectional extends LocalMaximalColouringAbstractMode {
//   override val mode: VertexProgram.Mode = VertexProgram.Bidirectional
  
// }