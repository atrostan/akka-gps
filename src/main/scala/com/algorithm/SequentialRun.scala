package com.algorithm

import scalax.collection.edge.Implicits._
import scalax.collection.Graph // or scalax.collection.mutable.Graph
import scalax.collection.GraphPredef._, scalax.collection.GraphEdge._
import scalax.collection.edge.WDiEdge

object SequentialRun {
  def apply[VertexIdT, MessageT, AccumulatorT, VertexValT](vertexProgram: VertexProgram[VertexIdT, Int, MessageT, AccumulatorT, VertexValT], graph: Graph[VertexIdT, WDiEdge])
    (initialStates: Map[graph.NodeT, VertexValT], initialActiveMap: Map[graph.NodeT, Boolean]): Map[graph.NodeT, VertexValT] = {

    type Vertex = graph.NodeT
    // var vertices: Seq[Int] = graph.nodes.toSeq.mzap({x:graph.NodeT => x.value})
    var vertices = graph.nodes
    var states = initialStates
    var activeMap = initialActiveMap
    val emptyMailboxes: Map[Vertex, Map[WDiEdge[Vertex], MessageT]] = vertices.map(vtx => (vtx, Map.empty[WDiEdge[Vertex], MessageT])).toMap
    var currentMailboxes: Map[Vertex, Map[WDiEdge[Vertex], MessageT]] = emptyMailboxes
    var nextMailboxes: Map[Vertex, Map[WDiEdge[Vertex], MessageT]] = emptyMailboxes
    var superstep = -1

    // println("Vertices: " + vertices)

    while(true) {
      // Superstep
      superstep += 1
      // println("Superstep: " + superstep)
      // println("States   : " + states)
      // println("Messages : " + currentMailboxes)
      var progressFlag = false
      
      // Iterate over vertices
      for {
        vtx <- vertices
        active = activeMap.getOrElse(vtx, false)
        mailbox = currentMailboxes.getOrElse(vtx, Map.empty)
        if active || mailbox.nonEmpty
      } {
        progressFlag = true

        // Gather
        var accumulator: Option[AccumulatorT] = None
        val messages: Map[WDiEdge[Vertex], MessageT] = currentMailboxes(vtx)
        for((edge, msg) <- messages) {
          val gatheredMsg = vertexProgram.gather(edge.weight.toInt, msg)
          accumulator match {
            case None => {
              accumulator = Some(gatheredMsg)
            }
            case Some(acc) => {
              accumulator = Some(vertexProgram.sum(acc, gatheredMsg))
            }
          }
        }

        // Apply
        val oldVal = states(vtx)
        val newVal = vertexProgram.apply(superstep, vtx.value, oldVal, accumulator)
        states = states.updated(vtx, newVal)

        // Scatter
        for {
          edge <- graph.edges
          if edge._1 == vtx
          dest = edge._2
          msg <- vertexProgram.scatter(vtx, oldVal, newVal)
        } {
          nextMailboxes = nextMailboxes.updated(dest, nextMailboxes(dest).updated(edge, msg))
        }

        val activation = !vertexProgram.voteToHalt(oldVal, newVal)
        activeMap = activeMap.updated(vtx, activation)
      }

      currentMailboxes = nextMailboxes
      nextMailboxes = emptyMailboxes
      if(progressFlag == false) {
        return states
      }
    }
    ??? // Unreachable
  }
}
