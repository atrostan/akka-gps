package com.example.algorithm

import org.scalatest._
import scalax.collection.edge.Implicits._
import scalax.collection.Graph // or scalax.collection.mutable.Graph
import scalax.collection.GraphPredef._, scalax.collection.GraphEdge._
import scalax.collection.edge.WDiEdge
import com.algorithm._
import scala.collection.immutable.ListMap


class SequentialTest extends FunSuite with Matchers {
  
  test("SSSP Graph 1") {
    val g: Graph[Int, WDiEdge] = Graph(
      0~>1 % 2,
      1~>2 % 2,
      0~>2 % 7,
      1~>4 % 6,
      2~>3 % 1,
      3~>4 % 6
    )
    val states = Map(
      g.Node(0) -> Integer.MAX_VALUE,
      g.Node(1) -> Integer.MAX_VALUE,
      g.Node(2) -> Integer.MAX_VALUE,
      g.Node(3) -> Integer.MAX_VALUE,
      g.Node(4) -> Integer.MAX_VALUE
    )
    val activeMap = Map(
      g.Node(0) -> true,
      g.Node(1) -> true,
      g.Node(2) -> true,
      g.Node(3) -> true,
      g.Node(4) -> true
    )
    val distances = Map(
      g.Node(0) -> 0,
      g.Node(1) -> 2,
      g.Node(2) -> 4,
      g.Node(3) -> 5,
      g.Node(4) -> 8
    )
    val results = SequentialRun(SSSP, g)(states, activeMap)
    results should be (distances)
  }

  test("SSSP Graph 2") {
    val g: Graph[Int, WDiEdge] = Graph(
      0~>1 % 500,
      0~>2 % 1000,
      0~>3 % 5,
      0~>4 % 1000,
      1~>2 % 100,
      3~>1 % 5,
      3~>5 % 7,
      4~>5 % 1000,
      5~>6 % 1000,
      6~>5 % 600,
      6~>7 % 1000,
      7~>2 % 1000
    )
    val states = ListMap(
      g.Node(0) -> Integer.MAX_VALUE,
      g.Node(1) -> Integer.MAX_VALUE,
      g.Node(2) -> Integer.MAX_VALUE,
      g.Node(3) -> Integer.MAX_VALUE,
      g.Node(4) -> Integer.MAX_VALUE,
      g.Node(5) -> Integer.MAX_VALUE,
      g.Node(6) -> Integer.MAX_VALUE,
      g.Node(7) -> Integer.MAX_VALUE,
    )
    val activeMap = Map(
      g.Node(0) -> true,
      g.Node(1) -> true,
      g.Node(2) -> true,
      g.Node(3) -> true,
      g.Node(4) -> true,
      g.Node(5) -> true,
      g.Node(6) -> true,
      g.Node(7) -> true,
    )
    val distances = ListMap(
      g.Node(0) -> 0,
      g.Node(1) -> 10,
      g.Node(2) -> 110,
      g.Node(3) -> 5,
      g.Node(4) -> 1000,
      g.Node(5) -> 12,
      g.Node(6) -> 1012,
      g.Node(7) -> 2012
    )
    val results = SequentialRun(SSSP, g)(states, activeMap)
    results should be (distances)
  }

  test("Local Maxima Colouring") {
    val g: Graph[Int, WDiEdge] = Graph(
      1~>2 % 1,
      1~>3 % 1,
      2~>1 % 1,
      2~>3 % 1,
      2~>4 % 1,
      3~>1 % 1,
      3~>2 % 1,
      3~>4 % 1,
      3~>5 % 1,
      4~>2 % 1,
      4~>3 % 1,
      5~>3 % 1,
    )
    val states: Map[g.NodeT, Option[Colour]] = g.nodes.map(v => (v, None)).toMap
    val activeMap = g.nodes.map(v => (v, true)).toMap
    val finalColours = ListMap(
      g.Node(1) -> Some(Colour(3)),
      g.Node(2) -> Some(Colour(2)),
      g.Node(3) -> Some(Colour(1)),
      g.Node(4) -> Some(Colour(0)),
      g.Node(5) -> Some(Colour(0)),
    )
    val results = SequentialRun(LocalMaximaColouring, g)(states, activeMap)
    results should be (finalColours)
  }
}