package com.algorithm

// Stateless
trait VertexProgram[VertexIdT, EdgeValT, MessageT, AccumulatorT, VertexValT] {
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
