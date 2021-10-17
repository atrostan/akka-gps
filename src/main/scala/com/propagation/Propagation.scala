//#full-example
package com.example


import akka.actor.typed.ActorRef
import akka.actor.typed.{ ActorSystem, PostStop }
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import scalax.collection.Graph
import scalax.collection.GraphBase
import scalax.collection.GraphPredef._, scalax.collection.GraphEdge._
import scala.collection.immutable.Vector
import com.example.VertexMain.StartProcessing // ASK Why does this work
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

/**
  Behviour for IdRequest message
    if acceptingRequests:
      disable acceptingRequests
      if hasNeighbours:
        message neighbors requesting info
      else:
        send response with own id to requestor
    else:
      send response with no id to requestor


  Behaviour for IdResponse message
    add given id/data to list of collected data
    if len list == number of neighbours:         // Could also check that the data contains all the neighbours
      send IdResponse to parent, containing list
      Behaviors.stopped

  */

object Vertex {
  sealed trait VertexMessage
  final case class Request(from: Int, replyTo: ActorRef[VertexMessage]) extends VertexMessage
  final case class Response(from: Int, data: Vector[Int]) extends VertexMessage
  final case class SendGraphInfo(outNeighborsMap: Map[Int,Set[Int]], vertexRefs: Vector[ActorRef[VertexMessage]]) extends VertexMessage // HACK since node data not sent (due to type problems)
  
  // Variables here are shared across instances
  // var data = ArrayBuffer[Int]()

  // TODO Alt design pattern
  // def apply(max: Int): Behavior[VertexMessage] = {
  //   doStuff(0, max)
  // }

  def apply(id: Int): Behavior[VertexMessage] = {
    // HACK graph data
    var outNeighborsMap: Map[Int,Set[Int]] = null
    var vertexRefs: Vector[ActorRef[VertexMessage]] = null

    var allData = ArrayBuffer[Int]()
    var acceptingRequests = true    // NOT ROBUST
    var parent = 0 // id of replyTo, since variable is not available when response needs to be run

    Behaviors
      .receive[VertexMessage] { (context, message) =>
        message match {
          case Request(from, replyTo) => 
            context.log.info("{} requested data from {}", from, id.toString())
            // if (acceptingRequests) {
            //   acceptingRequests = false
              parent = from
              if (outNeighborsMap(id).size > 0) {
                outNeighborsMap(id).foreach(neigh => {
                  val neighRef = vertexRefs(neigh)
                  neighRef ! Request(id, context.self)
                })
              } else {
                replyTo ! Response(id, Vector(id))
              }
            // } else { // happens when vertex is requested multiple timees
            //   replyTo ! Response(id, Vector())
            // }
            Behaviors.same
          case Response(from, data) => 
            context.log.info("{} sent response data to {}:   {}", from, id.toString(), data.toString())
            allData ++= data
            // Special case for source vertex = 0 //TODO extract away to system or parent
            if (id == 0) {
              println("vertex0 has", allData.toSet)
            }
            if (id == 0 && allData.toSet.size == 5) {
                println("COMPLETE. Result: {}", data)
                Behaviors.stopped // REVIEW
            } else if (id != 0 && data.size > 0) { // if the message has any data
              // add given id/data to list of collected data
              // allData ++= data
              // // Correctness check to know when to end
              // if (allData.size == outNeighborsMap(id).size) {         // Could also check that the data contains all the neighbours
              //   val parentRef = vertexRefs(parent)
              //   parentRef ! Response(id, allData.toVector)
              //   Behaviors.stopped // REVIEW
              // } else {
              //   Behaviors.same
              // }
              
              // Alternative response strategy: forward everything received
              val parentRef = vertexRefs(parent)
              parentRef ! Response(id, data.toVector :+ id)
              Behaviors.same
            } else { // NOTE else needed, otherwise scala implicit else returns Unit
              Behaviors.same
            }
          case SendGraphInfo(neighMap, allRefs) => 
            outNeighborsMap = neighMap
            vertexRefs = allRefs
            Behaviors.same
        }
      }
      // .receiveSignal {
      //   case (context, PostStop) =>
      //     context.log.info("Master Control Program stopped")
      //     Behaviors.stopped
      // }
  }
}

//#greeter-main
object VertexMain {
  final case class SayHello(name: String)
  final case class StartProcessing(nodes: Vector[Int], outNeighborsMap: Map[Int,Set[Int]])

  def apply(): Behavior[StartProcessing] =
    Behaviors.setup { context =>
      var allVertices = Vector[ActorRef[Vertex.VertexMessage]]()

      Behaviors.receiveMessage { message =>
        context.log.info("starter message {}", message)

        // Create all the vertices // TODO Split into separate message
        message.nodes.foreach(n => {
          println(n)
          allVertices = allVertices :+ context.spawn(Vertex(id = n), "vertex" + n.toString()) // save ref to vertex
        })
        context.log.info("list of all vertex actors {}", allVertices)
        // send all the graph data, now that all ActorRefs are available
        allVertices.foreach(actor => {
          actor ! Vertex.SendGraphInfo(message.outNeighborsMap, allVertices)
        })

        // Startup processing
        // val source: ActorRef[Vertex.VertexMessage] = context.spawn(Vertex(id = 0, outNeighborsMap = message.outNeighborsMap, vertexRefs = allVertices), "vertex0") 
        val source: ActorRef[Vertex.VertexMessage] = allVertices(0)
        val firstVertex: ActorRef[Vertex.VertexMessage] = allVertices(1)
        firstVertex ! Vertex.Request(0, source)
        Behaviors.same
      }
    }
}
//#greeter-main

//#main-class
object Propagation extends App {
  //#actor-system
  val greeterMain: ActorSystem[VertexMain.StartProcessing] = ActorSystem(VertexMain(), "GraphPropagationTest")
  //#actor-system

  val g = Graph(0~>1, 1~>2, 2~>3, 1~>3, 1~>5, 3~>5, 3~>4, 4~>4, 4~>5) // , 4~>4
  //NOTE this values on the right are wrong with vertex 0 
  println(g.order)                                // Int = 5
  println(g.graphSize)                            // Int = 8
  println(g.size)                                 // Int = 13
  println(g.totalDegree)                          // Int = 16
  println(g.degreeSet)                            // TreeSet(4, 3, 2)
  println(g.degreeNodeSeq(g.InDegree))            // List((4,3), (3,5), (2,1), (2,2), (2,4))
  println(g.degreeNodeSeq(g.OutDegree))            // List((4,3), (3,5), (2,1), (2,2), (2,4))
  println(g.degreeNodesMap)                       // Map(2->Set(2), 3->Set(5,1), 4->Set(3,4))
  println(g.degreeNodesMap(degreeFilter = _ > 3)) // Map(4 -> Set(3,4))
  println(g.nodes)                                // TODO
  
  // Convert list to Ints and create outNeighbour map to avoid needing g.NodeT
  val nodesVec = ArrayBuffer[Int]()
  var outNeighborsMap = Map[Int, Set[Int]]()
  g.nodes.foreach(n => {
    println(n)
    nodesVec += n.value

    var outNeighbors = Set[Int]()
    n.outNeighbors.foreach[Unit](neigh => outNeighbors += neigh.value)
    outNeighborsMap += (n.value -> outNeighbors)
  })                              // TODO

  // Convert to Map[Int, Set[Int]] to avoid needing g.NodeT
  // g.degreeNodesMap.foreach(kv => {
  //   var neighborSet = Set[Int]()
  //   kv._2.foreach[Unit](n => neighborSet += n.value)
  //   outNeighborsMap += (kv._1 -> neighborSet)
  // })

  greeterMain ! StartProcessing(nodesVec.toVector, outNeighborsMap)
}
//#main-class
//#full-example

