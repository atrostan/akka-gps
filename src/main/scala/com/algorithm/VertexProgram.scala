package com.algorithm

object VertexProgram {
  sealed trait Mode
  case object Outwards extends Mode // Send messages to out-neighbours
  case object Inwards extends Mode // Send messages to in-neighbours
  case object Bidirectional extends Mode // Send messages to both out-neighbours and in-neighbours
}

// Stateless
trait VertexProgram[VertexIdT, EdgeValT, MessageT, AccumulatorT, VertexValT] {

  val mode: VertexProgram.Mode

  def gather(edgeVal: EdgeValT, message: MessageT): AccumulatorT

  def sum(a: AccumulatorT, b: AccumulatorT): AccumulatorT

  def apply(
      superStepNumber: Int,
      thisVertexId: VertexIdT,
      oldVal: VertexValT,
      total: Option[AccumulatorT]
  ): VertexValT

  def scatter(thisVertexId: VertexIdT, oldVal: VertexValT, newVal: VertexValT): Option[MessageT]

  def voteToHalt(oldVal: VertexValT, newVal: VertexValT): Boolean

  val defaultVertexValue: VertexValT

  val defaultActivationStatus: Boolean
}
