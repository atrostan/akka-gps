package com.cluster.graph

import akka.actor.typed._
import akka.cluster.Member
import com.preprocessing.partitioning.oneDim.{Edge, Partitioning, Vertex}
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.ArrayBuffer

import scala.collection.mutable.ArrayBuffer

object ClusterShardingApp {

  // example of how to define a custom type
  // TODO use more type aliases in project
  //  type PartitionMap = collection.mutable.Map[Int, Int]
  //  object PartitionMap {
  //    def empty: PartitionMap = collection.mutable.Map.empty
  //    def apply(pairs: (Int,Int)*): PartitionMap = collection.mutable.Map(pairs:_*)
  //  }
  //
  //  val myPMap: PartitionMap = collection.mutable.Map[Int,Int]()

  def initGraphPartitioning(): Partitioning = {
    val edges = ArrayBuffer[Edge]()
    val nPartitions: Int = 4
    val nNodes: Int = 5
    val nEdges: Int = 10

    val v0 = Vertex(0)
    val v1 = Vertex(1)
    val v2 = Vertex(2)
    val v3 = Vertex(3)
    val v4 = Vertex(4)

    val e0 = Edge(v0, v1)
    val e1 = Edge(v0, v2)
    val e2 = Edge(v0, v3)
    val e3 = Edge(v1, v2)
    val e4 = Edge(v2, v0)
    val e5 = Edge(v2, v3)
    val e6 = Edge(v3, v0)
    val e7 = Edge(v3, v1)
    val e8 = Edge(v0, v4)
    val e9 = Edge(v4, v0)

    val es = Seq(e0, e1, e2, e3, e4, e5, e6, e7, e8, e9)
    edges.addAll(es)
    // create partitioning data structure
    val png = Partitioning(nPartitions, edges, nNodes, nEdges)
    png
  }

  def main(args: Array[String]): Unit = {
    val partitionMap = collection.mutable.Map[Int, Int]()
    if (args.isEmpty) {
      val ports = ArrayBuffer[Int](25252, 25253, 25254, 25255)
      startup("domainListener", ports.head - 1, partitionMap)

      var i: Int = 0;
      ports.foreach(p => {
        partitionMap(i) = p
        startup("shard", p, partitionMap)
        i += 1
      })

      startup("front", ports.last + 1, partitionMap)
    } else {
      require(args.size == 2, "Usage: role port")
      startup(args(0), args(1).toInt, partitionMap)
    }
  }

  def startup(role: String, port: Int, partitionMap: collection.mutable.Map[Int, Int]): Unit = {
    // Override the configuration of the port when specified as program argument
    val config = ConfigFactory
      .parseString(
        s"""
      akka.remote.artery.canonical.port=$port
      akka.cluster.roles = [$role]
      """)
      .withFallback(ConfigFactory.load("cluster"))

    var nodesUp = collection.mutable.Set[Member]()
    val png = initGraphPartitioning()

    if (role == "domainListener") {
      // enable ClusterMemberEventListener for logging purposes
      ActorSystem(ClusterMemberEventListener(nodesUp), "ClusterSystem", config)
    } else {
      val entityManager =
        ActorSystem[VertexEntityManager.Command](
          VertexEntityManager(partitionMap, png.mainArray),
          "ClusterSystem", config
        )

      if (role == "front") {
        // init mains and mirrors
        // TODO Decide on whether to put here or elsewhere, the conversion of neighbour Actor to a simple string/EntityId form. Maybe entityIds should be constructed here?
        // TODO Decide whether to pass Partition object or just id.
        for (main <- png.mainArray) entityManager ! VertexEntityManager.Initialize(main.id, main.partition.id, main.neighbors.map(n => new EntityId(n.id, n.partition.id)))

        // increment mains and their mirrors
        for (main <- png.mainArray) entityManager ! VertexEntityManager.AddOne(main.id, main.partition.id)
        for (main <- png.mainArray) entityManager ! VertexEntityManager.AddOne(main.id, main.partition.id)

        // see if increments have been propagated correctly to mirrors
        for (main <- png.mainArray) {
          entityManager ! VertexEntityManager.GetSum(main.id, main.partition.id)
          for (mirror <- main.mirrors) {
            entityManager ! VertexEntityManager.GetSum(mirror.id, mirror.partition.id)
          }
        }
      }
    }
  }
}
