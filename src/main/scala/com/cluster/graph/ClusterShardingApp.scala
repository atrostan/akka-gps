package com.cluster.graph

import akka.actor.typed._
import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.cluster.{ClusterEvent, Member}
import akka.util.Timeout
import com.cluster.graph.ClusterShardingApp.partitionMap
import com.graph.{Edge, Vertex}
import com.preprocessing.partitioning.oneDim.Partitioning
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable.ArrayBuffer
import com.cluster.graph.entity.{EntityId, MainEntity, MirrorEntity}
import com.cluster.graph.Init._

import scala.concurrent.{Await, Future}

object ClusterShardingApp {

  val partitionMap = collection.mutable.Map[Int, Int]()
  val partCoordMap = collection.mutable.Map[Int, Int]()
  val numberOfShards = ConfigFactory
    .load("cluster")
    .getInt("akka.cluster.sharding.number-of-shards")
  // example of how to define a custom type
  // TODO use more type aliases in project
  //  type PartitionMap = collection.mutable.Map[Int, Int]
  //  object PartitionMap {
  //    def empty: PartitionMap = collection.mutable.Map.empty
  //    def apply(pairs: (Int,Int)*): PartitionMap = collection.mutable.Map(pairs:_*)
  //  }
  //
  //  val myPMap: PartitionMap = collection.mutable.Map[Int,Int]()

  // Sample graph for partitioning and akka population test
  def initGraphPartitioning(nPartitions: Int): Partitioning = {
    val edges = ArrayBuffer[Edge]()
    val nNodes: Int = 8
    val nEdges: Int = 14

    val v0 = Vertex(0)
    val v1 = Vertex(1)
    val v2 = Vertex(2)
    val v3 = Vertex(3)
    val v4 = Vertex(4)
    val v5 = Vertex(5)
    val v6 = Vertex(6)
    val v7 = Vertex(7)

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
    val e10 = Edge(v5, v6)
    val e11 = Edge(v6, v7)
    val e12 = Edge(v7, v0)
    val e13 = Edge(v0, v7)

    val vs = ArrayBuffer(v0, v1, v2, v3, v4)

    val es = ArrayBuffer(e0, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13)
    // create partitioning data structure
    val png = Partitioning(nPartitions, es, nNodes, nEdges)
    println("Partitioning result:")
    println(png)
    png
  }

  def main(args: Array[String]): Unit = {

    val png = initGraphPartitioning(numberOfShards)

//    // create domainlistener
//    val domainListenerPort = 25251
//    val domainListenerRole = "domainListener"
//    var nodesUp = collection.mutable.Set[Member]()
//    val domainListenerConfig = createConfig(domainListenerRole, domainListenerPort)
//    val domainListener: ActorSystem[ClusterEvent.ClusterDomainEvent] = ActorSystem(ClusterMemberEventListener(nodesUp), "ClusterSystem", domainListenerConfig)
//
//    // create global coordinator
//
//    val shardPorts = ArrayBuffer[Int](25252, 25253, 25254, 25255)
//    val shardActors = ArrayBuffer[ActorSystem[EntityManager.Command]]()
//    val pcActors = ArrayBuffer[ActorSystem[PartitionCoordinator.Command]]()
//    var pid = 0
//    val nMains = png.mainArray.length
//    val nMirrors = png.mainArray.map(m => m.mirrors.length).sum
//
//    for (shardPort <- shardPorts) {
//
//      val shardConfig = createConfig("shard", shardPort)
//      val pcConfig = createConfig("partitionCoordinator", shardPort+ numberOfShards)
//
//      val entityManager = ActorSystem[EntityManager.Command](
//        EntityManager(partitionMap, png.mainArray),
//        "ClusterSystem", shardConfig
//      )
//      shardActors += entityManager
//
//      val mains = png.mainArray
//        .filter(m => m.partition.id == pid)
//        .map(m => new EntityId("Main", m.id, pid))
//        .toList
//
//      val pc = ActorSystem[PartitionCoordinator.Command](
//        PartitionCoordinator(mains, pid),
//        "ClusterSystem", pcConfig
//      )
//
//      pcActors += pc
//      pid+=1
//    }
//
//
//
//    for (a <- shardActors) {
//      println(a.path)
//      println(a.address)
//    }
//
//    for (a <- pcActors) {
//      println(a.path)
//      println(a.address)
//    }
//
//    val frontPort = 25260
//    val frontRole = "front"
//
//    val frontConfig = createConfig(frontRole, frontPort)
//    val entityManager = ActorSystem[EntityManager.Command](
//      EntityManager(partitionMap, png.mainArray),
//      "ClusterSystem", frontConfig
//    )
//    println(s"Initializing ${nMains} Mains and ${nMirrors} Mirrors...")
//    for (main <- png.mainArray) {
//      println(main)
//      entityManager ! EntityManager.Initialize(
//        MainEntity.getClass.toString(),
//        main.id,
//        main.partition.id,
//        main.neighbors.map(
//          n => new EntityId(MainEntity.getClass.toString(), n.id, n.partition.id)
//        )
//      )
//    }
//
//    val nMainsInitialized = getNMainsInitialized(entityManager)
//    val nMirrorsInitialized = getNMirrorsInitialized(entityManager)
//
//    println("Checking that all Mains, Mirrors have been initialized...")
////    assert(nMainsInitialized == nMains)
////    assert(nMirrorsInitialized == nMirrors)
//    // after initialization, each partition coordinator should broadcast its location to its mains
//    entityManager ! EntityManager.findPC()

    if (args.isEmpty) {
      val shardPorts = ArrayBuffer[Int](25252, 25253, 25254, 25255)
      startup("domainListener", shardPorts.head - 1, partitionMap, png, -1)

      var partitionId: Int = 0;

      for (shardPort <- shardPorts) {
        partitionMap(partitionId) = shardPort
        startup("shard", shardPort, partitionMap, png, partitionId)
        partitionId += 1
      }

      startup("front", shardPorts.last + numberOfShards + 1, partitionMap, png, -1)
    } else {
      require(args.size == 2, "Usage: role port")
      startup(args(0), args(1).toInt, partitionMap, png, -1)
    }
  }

  def createConfig(role: String, port: Int): Config = {
    val config = ConfigFactory
      .parseString(
        s"""
      akka.remote.artery.canonical.port=$port
      akka.cluster.roles = [$role]
      """)
      .withFallback(ConfigFactory.load("cluster"))
    config
  }

  def startup(
               role: String,
               port: Int,
               partitionMap: collection.mutable.Map[Int, Int],
               png: => Partitioning,
               partitionId: Int
             ): Unit = {
    // Override the configuration of the port when specified as program argument
    val config = ConfigFactory
      .parseString(
        s"""
      akka.remote.artery.canonical.port=$port
      akka.cluster.roles = [$role]
      """)
      .withFallback(ConfigFactory.load("cluster"))

    var nodesUp = collection.mutable.Set[Member]()
    val nMains = png.mainArray.length
    val nMirrors = png.mainArray.map(m => m.mirrors.length).sum


    if (role == "domainListener") {
      println(s"Running ${role} on ${port}")
      // enable ClusterMemberEventListener for logging purposes
      ActorSystem(ClusterMemberEventListener(nodesUp), "ClusterSystem", config)
    } else {

      // create an entity manager and partitionCoordinator for this partition
      val entityManager = ActorSystem[EntityManager.Command](
        EntityManager(partitionMap, png.mainArray),
        "ClusterSystem", config
      )
      val partitionCoordinator = initPartitionCoordinator(partitionId, port + numberOfShards, png)

      if (role == "front") {
        // init mains and mirrors
        // TODO Decide whether to pass Partition object or just id.
        // TODO Decide on whether to put here or elsewhere, the conversion of neighbour Actor to a simple string/EntityId form. Maybe entityIds should be constructed here?
        // TODO Need to distinguish if neighbor is main or mirror. For passes along info to EntityManager
        println(s"Initializing ${nMains} Mains and ${nMirrors} Mirrors...")
        for (main <- png.mainArray) {
          entityManager ! EntityManager.Initialize(
            MainEntity.getClass.toString(),
            main.id,
            main.partition.id,
            main.neighbors.map(
              n => new EntityId(MainEntity.getClass.toString(), n.id, n.partition.id)
            )
          )
        }
        val nMainsInitialized = getNMainsInitialized(entityManager)
        val nMirrorsInitialized = getNMirrorsInitialized(entityManager)

        println("Checking that all Mains, Mirrors have been initialized...")
        assert(nMainsInitialized == nMains)
        assert(nMirrorsInitialized == nMirrors)
        // after initialization, each partition coordinator should broadcast its location to its mains
        entityManager ! EntityManager.findPC()

      }
//      println("broadcasting location...")
//      partitionCoordinator ! PartitionCoordinator.BroadcastLocation()


      //      if (role == "front") {
      //        // increment mains and their mirrors
      //        for (main <- png.mainArray) entityManager ! EntityManager.AddOne(MainEntity.getClass.toString(), main.id, main.partition.id)
      //        for (main <- png.mainArray) entityManager ! EntityManager.AddOne(MainEntity.getClass.toString(), main.id, main.partition.id)
      //
      //        // see if increments have been propagated correctly to mirrors
      //        for (main <- png.mainArray) {
      //          entityManager ! EntityManager.GetSum(MainEntity.getClass.toString(), main.id, main.partition.id)
      //          for (mirror <- main.mirrors) {
      //            entityManager ! EntityManager.GetSum(MirrorEntity.getClass.toString(), mirror.id, mirror.partition.id)
      //          }
      //        }
      //      }


    }
  }
}
