package com.cluster.graph

import akka.actor.typed._
import akka.cluster.{ClusterEvent, Member}
import com.Typedefs.{EMRef, GCRef, PCRef}
import com.cluster.graph.Init._
import com.cluster.graph.PartitionCoordinator.BroadcastLocation
import com.cluster.graph.entity.{EntityId, MainEntity, VertexEntityType}
import com.preprocessing.partitioning.oneDim.Partitioning
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable.ArrayBuffer

object ClusterShardingApp {

  val partitionMap = collection.mutable.Map[Int, Int]()
  val partCoordMap = collection.mutable.Map[Int, Int]()
  val numberOfShards = ConfigFactory
    .load("cluster")
    .getInt("akka.cluster.sharding.number-of-shards")

  // number of nodes in the cluster =
  // (partitionCoordinator/shard) * nPartitions + domainlistener + front/globalCoordinator
  val nNodes = numberOfShards + 2

  def main(args: Array[String]): Unit = {

    val png = initGraphPartitioning(numberOfShards)
    val nodesUp = collection.mutable.Set[Member]()

    println(s"Initializing cluster with ${nNodes} compute nodes")

    println("Initializing domain listener")
    val domainListenerPort = 25251
    val domainListenerRole = "domainListener"
    val domainListenerConfig = createConfig(domainListenerRole, domainListenerPort)
    val domainListener: ActorSystem[ClusterEvent.ClusterDomainEvent] = ActorSystem(
      ClusterMemberEventListener(nodesUp, nNodes),
      "ClusterSystem",
      domainListenerConfig
    )

    val shardPorts = ArrayBuffer[Int](25252, 25253, 25254, 25255)
    val shardActors = ArrayBuffer[ActorSystem[EntityManager.Command]]()
    var pid = 0
    val nMains = png.mainArray.length
    val nMirrors = png.mainArray.map(m => m.mirrors.length).sum

    for (shardPort <- shardPorts) {
      val pcPort = shardPort + numberOfShards
      val shardConfig = createConfig("shard", shardPort)
      val pcConfig = createConfig("partitionCoordinator", pcPort)

      partitionMap(pid) = shardPort
      partCoordMap(pid) = pcPort

      val entityManager = ActorSystem[EntityManager.Command](
        EntityManager(partitionMap, png.mainArray, pid),
        "ClusterSystem",
        shardConfig
      )
      shardActors += entityManager

      pid += 1
    }

    val frontPort = shardPorts.last + 1
    val frontRole = "front"

    val frontConfig = createConfig(frontRole, frontPort)
    val entityManager = ActorSystem[EntityManager.Command](
      EntityManager(partitionMap, png.mainArray, pid),
      "ClusterSystem",
      frontConfig
    )

    println("Blocking until all cluster members are up...")
    blockUntilAllMembersUp(domainListener, nNodes)

    println("Blocking until all EntityManagers are registered...")
    val emRefs: collection.mutable.Map[Int, EMRef] =
      blockUntilAllRefsRegistered[EntityManager.Command](
        entityManager,
        "entityManager",
        numberOfShards + 1
      )
    println(s"Registered ${emRefs.size} EntityManagers.")

    println("Blocking until all PartitionCoordinators are registered...")
    for (pid <- 0 until numberOfShards) {
      val emRef: EMRef = emRefs(pid)
      emRef ! EntityManager.SpawnPC(pid)
    }
    val pcRefs: collection.mutable.Map[Int, PCRef] =
      blockUntilAllRefsRegistered[PartitionCoordinator.Command](
        entityManager,
        "partitionCoordinator",
        numberOfShards
      )
    println(s"Registered ${pcRefs.size} PartitionCoordinators.")

    println("Blocking until the Global Coordinator is initialized and registered...")
    entityManager ! EntityManager.SpawnGC()
    val gcRefs: collection.mutable.Map[Int, GCRef] =
      blockUntilAllRefsRegistered[GlobalCoordinator.Command](entityManager, "globalCoordinator", 1)
    val gcRef = gcRefs(0)

    blockInitGlobalCoordinator(gcRef, entityManager.scheduler, pcRefs, nNodes)
    println(s"Registered the GlobalCoordinator.")
    println("Broadcasting the Global Coordinator address to all Partition Coordinators")
    broadcastGCtoPCs(gcRef, entityManager.scheduler)

    println(s"Initializing ${nMains} Mains and ${nMirrors} Mirrors...")
    for (main <- png.mainArray) {
      println(main)
      entityManager ! EntityManager.Initialize(
        VertexEntityType.Main.toString(),
        main.id,
        main.partition.id,
        main.neighbors.map(n =>
          new EntityId(VertexEntityType.Main.toString(), n.id, n.partition.id)
        )
      )
    }

    val nMainsInitialized = getNMainsInitialized(entityManager)
    val nMirrorsInitialized = getNMirrorsInitialized(entityManager)

    println("Checking that all Mains, Mirrors have been initialized...")
    assert(nMainsInitialized == nMains)
    assert(nMirrorsInitialized == nMirrors)
    //     after initialization, each partition coordinator should broadcast its location to its mains
    for ((pid, pcRef) <- pcRefs) {
      println(s"PC${pid} Broadcasting location to its main")
      pcRef ! BroadcastLocation()
    }

    //    if (args.isEmpty) {
    //      val shardPorts = ArrayBuffer[Int](25252, 25253, 25254, 25255)
    //      startup("domainListener", shardPorts.head - 1, partitionMap, png, -1)
    //
    //      var partitionId: Int = 0;
    //
    //      for (shardPort <- shardPorts) {
    //        partitionMap(partitionId) = shardPort
    //        startup("shard", shardPort, partitionMap, png, partitionId)
    //        partitionId += 1
    //      }
    //
    //      startup("front", shardPorts.last + numberOfShards + 1, partitionMap, png, -1)
    //    } else {
    //      require(args.size == 2, "Usage: role port")
    //      startup(args(0), args(1).toInt, partitionMap, png, -1)
    //    }

  }

  def createConfig(role: String, port: Int): Config = {
    val config = ConfigFactory
      .parseString(s"""
      akka.remote.artery.canonical.port = $port
      akka.cluster.roles = [$role]
      akka.cluster.seed-nodes = [
        "akka://ClusterSystem@127.0.0.1:25251"
      ]
      akka.cluster.role.shard.min-nr-of-members = $numberOfShards

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
      .parseString(s"""
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
      ActorSystem(ClusterMemberEventListener(nodesUp, nNodes), "ClusterSystem", config)
    } else {

      // create an entity manager and partitionCoordinator for this partition
      val entityManager = ActorSystem[EntityManager.Command](
        EntityManager(partitionMap, png.mainArray, -1),
        "ClusterSystem",
        config
      )
//      val partitionCoordinator = initPartitionCoordinator(partitionId, port + numberOfShards, png)

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
            main.neighbors.map(n =>
              new EntityId(MainEntity.getClass.toString(), n.id, n.partition.id)
            )
          )
        }
        val nMainsInitialized = getNMainsInitialized(entityManager)
        val nMirrorsInitialized = getNMirrorsInitialized(entityManager)

        println("Checking that all Mains, Mirrors have been initialized...")
        println(partitionMap)
        assert(nMainsInitialized == nMains)
        assert(nMirrorsInitialized == nMirrors)

        // after initialization, each partition coordinator should broadcast its location to its mains
//        entityManager ! EntityManager.askPCRefFromReceptionist()

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
