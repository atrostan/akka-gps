package com.helloWorld

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props

import akka.actor.typed._
import akka.actor.typed.scaladsl._
import akka.cluster.ClusterEvent._
import akka.cluster.MemberStatus
import akka.cluster.typed._

import scalax.collection.edge.Implicits._
import scalax.collection.Graph // or scalax.collection.mutable.Graph
import scalax.collection.GraphPredef._, scalax.collection.GraphEdge._

import com.typesafe.config.ConfigFactory

class HelloActor extends Actor {
  def receive = {
    case "hello" => println("hello back at you")
    case _       => println("huh?")
  }
}

object Main extends App {
  val system = ActorSystem("HelloSystem")
  // default Actor constructor
  val helloActor = system.actorOf(Props[HelloActor], name = "helloactor")

  // 1~>2 % 4 --> a directed edge from 1 to 2 with weight 4
  // 1~2 --> an undirected edge between 1 and 2

  val g = Graph(1~>2, 2~>3, 1~>3, 1~>5, 3~>5, 3~>4, 4~>4, 4~>5)
  println(g.order)                                // Int = 5
  println(g.graphSize)                            // Int = 8
  println(g.size)                                 // Int = 13
  println(g.totalDegree)                          // Int = 16
  println(g.degreeSet)                            // TreeSet(4, 3, 2)
  println(g.degreeNodeSeq(g.InDegree))            // List((4,3), (3,5), (2,1), (2,2), (2,4))
  println(g.degreeNodeSeq(g.OutDegree))            // List((4,3), (3,5), (2,1), (2,2), (2,4))
  println(g.degreeNodesMap)                       // Map(2->Set(2), 3->Set(5,1), 4->Set(3,4))
  println(g.degreeNodesMap(degreeFilter = _ > 3)) // Map(4 -> Set(3,4))

  val nodes = g.nodes
  nodes.foreach(n => println(n))

  // config a cluster
  val configSystem1 = ConfigFactory.parseString(s"""
    akka.loglevel = DEBUG
    #config-seeds
    akka {
      actor {
        provider = "cluster"
      }
      remote.artery {
        canonical {
          hostname = "127.0.0.1"
          port = 2551
        }
      }

      cluster {
        seed-nodes = [
          "akka://ClusterSystem@127.0.0.1:2551",
          "akka://ClusterSystem@127.0.0.1:2552"]

        downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
      }
    }
    #config-seeds
     """)

  helloActor ! "hello"
  helloActor ! "buenos dias"
}