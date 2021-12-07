package com.algorithm

object VertexProgram {
  sealed trait Mode
  case object Outwards extends Mode // Send messages to out-neighbours
  case object Inwards extends Mode // Send messages to in-neighbours
  case object Bidirectional extends Mode // Send messages to both out-neighbours and in-neighbours
}


/**
  * Specification of "Think-Like-A-Vertex" program that is run at each vertex
  * in a superstep.
  * VertexPrograms are stateless.
  */
trait VertexProgram[EdgeValT, MessageT, AccumulatorT, VertexValT] {

  // Map messages into accumulator values
  def gather(edgeVal: EdgeValT, message: MessageT): AccumulatorT

  // Combine accumulator values
  def sum(a: AccumulatorT, b: AccumulatorT): AccumulatorT

  // Compute new local state for this vertex based on accumulated sum
  def apply(
      superStepNumber: Int,
      thisVertex: VertexInfo,
      oldVal: VertexValT,
      total: Option[AccumulatorT]
  ): VertexValT

  // Optionally generate a message to send to neighbours
  def scatter(superStepNumber: Int, thisVertex: VertexInfo, oldVal: VertexValT, newVal: VertexValT): Option[MessageT]

  // Whether to deactivate this vertex at the end of this superstep
  def deactivateSelf(superStepNumber: Int, oldVal: VertexValT, newVal: VertexValT): Boolean

  // Initial starting state
  val defaultVertexValue: VertexValT

  // Initial starting activation status
  val defaultActivationStatus: Boolean

  // Whether messages are sent along out-edges, in-edges, or both
  val mode: VertexProgram.Mode
}

case class VertexInfo(
  id: Int,
  degree: Int
)