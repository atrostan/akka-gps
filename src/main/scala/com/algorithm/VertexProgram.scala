package com.algorithm

// Stateless
trait VertexProgram[VertexId, EdgeVal, Message, Accumulator, VertexVal] {
  def gather(edgeVal: EdgeVal, message: Message): Accumulator

  def sum(a: Accumulator, b: Accumulator): Accumulator

  def apply(thisVertex: VertexId, oldVal: VertexVal, total: Option[Accumulator]): VertexVal

  def scatter(oldVal: VertexVal, newVal: VertexVal): Option[Message]

  def voteToHalt(oldVal: VertexVal, newVal: VertexVal): Boolean
}




