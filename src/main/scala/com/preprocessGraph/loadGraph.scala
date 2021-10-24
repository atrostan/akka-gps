package com.preprocessGraph

import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map


// Takes in newline separated list of vertex-neighbour pairs, optionally with weights. Creates both an outNeighbor map and a list of edges
object LoadGraph extends App {
  val bufferedSource = Source.fromFile("src/main/resources/email-Eu-core.txt")
  
  var outNeighborMap = Map[Int, ArrayBuffer[Int]]() // TODO Implement support for weights
  final case class Edge(src: Int, dst: Int, weight: Int)
  var edges = ArrayBuffer[Edge]()
  
  val isWeighted = false // TODO Settable via args or system parameters

  for (line <- bufferedSource.getLines) {
    val vertices = line.split("\\W+")
    // val Array(a,b,_*) = line.split("\\W+") // alternative for getting named elems

    if (vertices.length > 1) {
      val source = vertices(0)
      val dest = vertices(1)
      // println("updating source and dest", source, dest)
      
      // Same as comment below but fancy
      outNeighborMap.updateWith(source.toInt)({
        case Some(list) => Some(list.append(dest.toInt))
        case None => Some(ArrayBuffer(dest.toInt))
      })

      // if (outNeighborMap.contains(vertices(0).toInt)) {
      //   outNeighborMap(source.toInt) = outNeighborMap(source.toInt) += dest.toInt
      // } else {
      //   outNeighborMap(source.toInt) = ArrayBuffer(dest.toInt)
      // }

      if (isWeighted) {
        val weight = vertices(2)
        edges += Edge(source.toInt, dest.toInt, weight.toInt)
      } else {
        edges += Edge(source.toInt, dest.toInt, 1)
      }
    }
  }
  println(edges(45)) // Edge(62,63,1)

  bufferedSource.close
}