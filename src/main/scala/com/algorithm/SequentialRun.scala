package com.algorithm

import scalax.collection.Graph
import scalax.collection.edge.WDiEdge

object SequentialRun {
  def apply[VertexIdT, MessageT, AccumulatorT, VertexValT](
      vertexProgram: VertexProgram[VertexIdT, Int, MessageT, AccumulatorT, VertexValT],
      graph: Graph[VertexIdT, WDiEdge]
  )(
      initialStates: Map[graph.NodeT, VertexValT],
      initialActiveMap: Map[graph.NodeT, Boolean]
  ): Map[graph.NodeT, VertexValT] = {

    type Vertex = graph.NodeT
    // var vertices: Seq[Int] = graph.nodes.toSeq.mzap({x:graph.NodeT => x.value})
    var vertices = graph.nodes
    var states = vertices.map(v => (v -> vertexProgram.defaultVertexValue)).toMap
    var activeMap = vertices.map(v => (v -> vertexProgram.defaultActivationStatus)).toMap

    type Mailbox = Map[WDiEdge[Vertex], MessageT]

    val emptyMailbox: Mailbox = Map.empty
    val emptyMailboxes: Map[Vertex, Mailbox] = vertices.map(vtx => (vtx, emptyMailbox)).toMap
    var currentMailboxes: Map[Vertex, Mailbox] = emptyMailboxes
    var nextMailboxes: Map[Vertex, Mailbox] = emptyMailboxes
    var superstep = -1

    // println("Vertices: " + vertices)

    def sendMessage(dest: Vertex, edge: graph.EdgeT, msg: MessageT): Unit = {
      nextMailboxes = nextMailboxes.updated(dest, nextMailboxes(dest).updated(edge, msg))
    }
    def outEdges(src: Vertex): Iterable[graph.EdgeT] = {
      graph.edges.filter(edge => edge._1 == src)
    }

    var progressFlag = true

    while (progressFlag) {
      // Superstep
      superstep += 1
      // println("Superstep: " + superstep)
      // println("States   : " + states)
      // println("Messages : " + currentMailboxes)
      progressFlag = false

      // Iterate over vertices
      for {
        vtx <- vertices
        active = activeMap.getOrElse(vtx, false)
        mailbox = currentMailboxes.getOrElse(vtx, Map.empty)
        if active || mailbox.nonEmpty
      } {
        progressFlag = true

        // Gather
        val messages: Map[WDiEdge[Vertex], MessageT] = currentMailboxes(vtx)
        val finalAccumulator = messages.foldLeft[Option[AccumulatorT]](None) {
          case (accOption, (edge, msg)) => {
            val gatheredMsg = vertexProgram.gather(edge.weight.toInt, msg)
            accOption match {
              case None           => Some(gatheredMsg)
              case Some(accSoFar) => Some(vertexProgram.sum(accSoFar, gatheredMsg))
            }
          }
        }

        // Apply
        val oldVal = states(vtx)
        val newVal = vertexProgram.apply(superstep, vtx.value, oldVal, finalAccumulator)
        states = states.updated(vtx, newVal)

        // Scatter
        for {
          edge <- outEdges(vtx)
          dest = edge._2
          msg <- vertexProgram.scatter(vtx, oldVal, newVal)
        } {
          sendMessage(dest, edge, msg)
        }

        val activation = !vertexProgram.voteToHalt(oldVal, newVal)
        activeMap = activeMap.updated(vtx, activation)
      }

      currentMailboxes = nextMailboxes
      nextMailboxes = emptyMailboxes
    }
    states
  }
}
