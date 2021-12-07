package com.algorithm

case class Colour(num: Int) {
  require(num >= -1)
}
object Colour {
  val Blank = Colour(-1)
}

object LocalMaximalColouring extends VertexProgram[Int, Int, Int, Colour] {
  
  override def gather(edgeVal: Int, message: Int): Int = message

  override def sum(a: Int, b: Int): Int = Math.max(a, b)

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

  override def scatter(superStepNumber: Int, thisVertex: VertexInfo, oldVal: Colour, newVal: Colour): Option[Int] = {
    if (newVal == Colour.Blank) Some(thisVertex.id)
    else None
  }

  override def deactivateSelf(superStepNumber: Int, oldVal: Colour, newVal: Colour): Boolean = (newVal != Colour.Blank)

  override val defaultVertexValue: Colour = Colour(-1)

  override val defaultActivationStatus: Boolean = true

  override val mode = VertexProgram.Outwards
}

// object LocalMaximaColouring extends LocalMaximalColouringAbstractMode {
//   override val mode = VertexProgram.Outwards
// }

// object LocalMaximaColouringBidirectional extends LocalMaximalColouringAbstractMode {
//   override val mode: VertexProgram.Mode = VertexProgram.Bidirectional
  
// }