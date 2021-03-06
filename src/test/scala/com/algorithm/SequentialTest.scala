package com.algorithm

import org.scalatest._
import scalax.collection.Graph
import scalax.collection.GraphPredef._
import scalax.collection.edge.Implicits._
import scalax.collection.edge.WDiEdge

import scala.collection.immutable.ListMap

class SequentialTest extends FunSuite with Matchers {

  test("SSSP Graph 1") {
    val g: Graph[Int, WDiEdge] = Graph(
      0 ~> 1 % 2,
      1 ~> 2 % 2,
      0 ~> 2 % 7,
      1 ~> 4 % 6,
      2 ~> 3 % 1,
      3 ~> 4 % 6
    )
    val distances = Map(
      g.Node(0) -> 0,
      g.Node(1) -> 2,
      g.Node(2) -> 4,
      g.Node(3) -> 5,
      g.Node(4) -> 8
    )
    val results = SequentialRun(SSSP, g)
    results should be(distances)
  }

  test("SSSP Graph 2") {
    val g: Graph[Int, WDiEdge] = Graph(
      0 ~> 1 % 500,
      0 ~> 2 % 1000,
      0 ~> 3 % 5,
      0 ~> 4 % 1000,
      1 ~> 2 % 100,
      3 ~> 1 % 5,
      3 ~> 5 % 7,
      4 ~> 5 % 1000,
      5 ~> 6 % 1000,
      6 ~> 5 % 600,
      6 ~> 7 % 1000,
      7 ~> 2 % 1000
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
    val results = SequentialRun(SSSP, g)
    results should be(distances)
  }

  test("Local Maxima Colouring") {
    val g: Graph[Int, WDiEdge] = Graph(
      1 ~> 2 % 1,
      1 ~> 3 % 1,
      2 ~> 1 % 1,
      2 ~> 3 % 1,
      2 ~> 4 % 1,
      3 ~> 1 % 1,
      3 ~> 2 % 1,
      3 ~> 4 % 1,
      3 ~> 5 % 1,
      4 ~> 2 % 1,
      4 ~> 3 % 1,
      5 ~> 3 % 1
    )
    val finalColours = ListMap(
      g.Node(1) -> Colour(3),
      g.Node(2) -> Colour(2),
      g.Node(3) -> Colour(1),
      g.Node(4) -> Colour(0),
      g.Node(5) -> Colour(0)
    )
    val results = SequentialRun(LocalMaximaColouring, g)
    results should be(finalColours)
  }

  test("Local Maxima Colouring - Bidirectional") {
    val g: Graph[Int, WDiEdge] = Graph(
      1~>2 % 1,
      2~>3 % 1,
      2~>4 % 1,
      3~>1 % 1,
      3~>4 % 1,
      5~>3 % 1,
    )
    val finalColours = ListMap(
      g.Node(1) -> Colour(3),
      g.Node(2) -> Colour(2),
      g.Node(3) -> Colour(1),
      g.Node(4) -> Colour(0),
      g.Node(5) -> Colour(0),
    )
    val results = SequentialRun(LocalMaximaColouring, g)
    results should be (finalColours)
  }

  test("WCC") {
    val g: Graph[Int, WDiEdge] = Graph(
      // Component 1
      1~>2 % 1,
      2~>3 % 1,
      2~>4 % 1,
      3~>1 % 1,
      3~>4 % 1,
      5~>3 % 1,
      // Component 2
      6~>7 % 1,
      7~>8 % 1,
      7~>9 % 1,
      8~>6 % 1,
      8~>9 % 1,
      10~>8 % 1,
      // Component 3
      11~>12 % 1,
      12~>13 % 1,
      12~>14 % 1,
      13~>11 % 1,
      13~>14 % 1,
      15~>13 % 1,
    )
    val finalComponents = ListMap(
      g.Node(1) -> 1,
      g.Node(2) -> 1,
      g.Node(3) -> 1,
      g.Node(4) -> 1,
      g.Node(5) -> 1,
      g.Node(6) -> 6,
      g.Node(7) -> 6,
      g.Node(8) -> 6,
      g.Node(9) -> 6,
      g.Node(10) -> 6,
      g.Node(11) -> 11,
      g.Node(12) -> 11,
      g.Node(13) -> 11,
      g.Node(14) -> 11,
      g.Node(15) -> 11,
    )
    val results = SequentialRun(WCC, g)
    results should be (finalComponents)
  }

  test("PageRank 1") {
    val g: Graph[Int, WDiEdge] = Graph(
      0 ~> 1 % 1,
      0 ~> 2 % 1,
      1 ~> 2 % 1,
      2 ~> 0 % 1,
      3 ~> 2 % 1,
    )
    val results = SequentialRun(new PageRank(40), g)
    val normalized = for((vtx, rank) <- results) yield (vtx -> rank/g.nodes.size)
    val expected = ListMap(
      g.Node(0) -> 0.372,
      g.Node(1) -> 0.196,
      g.Node(2) -> 0.394,
      g.Node(3) -> 0.037
    )
    // object EpsilonMatcher extends EpsilonCloseMatchers(0.01)
    normalized should EpsilonCloseMatching.BeEpsilonClose[g.NodeT](0.01)(expected)
  }

  test("PageRank 2") {
    val g: Graph[Int, WDiEdge] = Graph(
      0 ~> 1 % 1,
      0 ~> 2 % 1,
      0 ~> 3 % 1,
      1 ~> 0 % 1,
      2 ~> 0 % 1,
      3 ~> 0 % 1,
      3 ~> 4 % 1,
      3 ~> 5 % 1,
      3 ~> 6 % 1,
      4 ~> 0 % 1,
      5 ~> 0 % 1,
      5 ~> 2 % 1,
      6 ~> 3 % 1,
      7 ~> 5 % 1,
      7 ~> 6 % 1,
    )
    val results = SequentialRun(new PageRank(40), g)
    val normalized = for((vtx, rank) <- results) yield (vtx -> rank/g.nodes.size)
    val expected = ListMap(
      g.Node(0) -> 0.358,
      g.Node(1) -> 0.120,
      g.Node(2) -> 0.147,
      g.Node(3) -> 0.174,
      g.Node(4) -> 0.056,
      g.Node(5) -> 0.064,
      g.Node(6) -> 0.064,
      g.Node(7) -> 0.019
    )
    normalized should EpsilonCloseMatching.BeEpsilonClose[g.NodeT](0.001)(expected)
  }


}
