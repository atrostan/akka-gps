package com.cluster.graph

import akka.actor.typed.{ActorRefResolver, ActorSystem, Scheduler}
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.cluster.ClusterEvent
import akka.cluster.sharding.typed.scaladsl.EntityRef
import akka.util.Timeout
import com.Typedefs.{GCRef, PCRef}
import com.cluster.graph.ClusterShardingApp.{partCoordMap, partitionMap}
import com.cluster.graph.GlobalCoordinator.GlobalCoordinatorKey
import com.cluster.graph.entity.{EntityId, MainEntity, MirrorEntity, VertexEntity}
import com.graph.{Edge, Vertex}
import com.preprocessing.partitioning.oneDim.Partitioning
import com.typesafe.config.{Config, ConfigFactory}

import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object Init {
  val waitTime = 10 seconds
  implicit val timeout: Timeout = waitTime

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

  def initEntityManager(png: Partitioning, config: Config): ActorSystem[EntityManager.Command] = {
    ActorSystem[EntityManager.Command](
      EntityManager(partitionMap, png.mainArray),
      "ClusterSystem", config
    )
  }

  def blockInitPartitionCoordinator(
                                     pc: ActorSystem[PartitionCoordinator.Command],
                                     mains: List[EntityId],
                                     partitionId: Int
                                   ): Int = {
    implicit val scheduler = pc.scheduler
    val future: Future[PartitionCoordinator.InitResponse] = pc.ask(ref =>
      PartitionCoordinator.Initialize(
        mains,
        partitionId,
        ref
      )
    )
    val pcInitResult = Await.result(future, waitTime)
    pcInitResult match {
      case PartitionCoordinator.InitResponse(message) =>
        println(message)
        1
      case _ =>
        println(s"Failed to init PartitionCoordinator for partition ${partitionId}")
        0
    }
  }

  def initPartitionCoordinator(
                                partitionId: Int,
                                port: Int,
                                png: Partitioning
                              ): ActorSystem[PartitionCoordinator.Command] = {
    println(s"pc on port: ${port}")
    var nPartitionCoordinatorsInitialized = 0
    // create a partition coordinator
    val partitionCoordinatorConfig = ConfigFactory
      .parseString(
        s"""
      akka.remote.artery.canonical.port=${port}
      akka.cluster.roles = [partitionCoordinator]
      """)
      .withFallback(ConfigFactory.load("cluster"))
    if (partitionId > 0) partCoordMap(port) = partitionId
    val mains = png.mainArray
      .filter(m => m.partition.id == partitionId)
      .map(m => new EntityId("Main", m.id, partitionId))
      .toList
    println(mains)
    val pc = ActorSystem[PartitionCoordinator.Command](
      PartitionCoordinator(mains, partitionId),
      "ClusterSystem", partitionCoordinatorConfig
    )
    nPartitionCoordinatorsInitialized += blockInitPartitionCoordinator(
      pc, mains, partitionId
    )
    println(nPartitionCoordinatorsInitialized)
    pc
  }

  def getNMainsInitialized(
                            entityManager: ActorSystem[EntityManager.Command],
                          ): Int = {
    // check that all mains have been correctly initialized
    implicit val scheduler = entityManager.scheduler
    val future: Future[EntityManager.NMainsInitResponse] = entityManager.ask(ref => EntityManager.GetNMainsInitialized(ref))
    val result = Await.result(future, waitTime)
    result match {
      case EntityManager.NMainsInitResponse(totalMainsInitialized) =>
        totalMainsInitialized
      case _ =>
        println("Failed to get number of initialized mains")
        -1
    }
  }

  def getNMirrorsInitialized(
                              entityManager: ActorSystem[EntityManager.Command],
                            ): Int = {
    // check that all mains have been correctly initialized
    //    entityManager ! EntityManager.GetNMainsInitialized()

    implicit val scheduler = entityManager.scheduler
    val future: Future[EntityManager.NMirrorsInitResponse] = entityManager.ask(ref => EntityManager.GetNMirrorsInitialized(ref))
    val result = Await.result(future, waitTime)
    result match {
      case EntityManager.NMirrorsInitResponse(totalMirrorsInitialized) =>
        totalMirrorsInitialized
      case _ =>
        println("Failed to get number of initialized mains")
        -1
    }
  }
  // initialize the global coordinator with the references to all the
  // partition coordinator and the number of nodes in the graph

  def blockInitGlobalCoordinator(
                                  gc: ActorSystem[GlobalCoordinator.Command],
                                  pcRefs: collection.mutable.Map[Int, PCRef],
                                  nNodes: Int
                                ): Unit = {
    implicit val scheduler = gc.scheduler

    val f: Future[GlobalCoordinator.InitResponse] = gc.ask(ref =>
      GlobalCoordinator.Initialize(pcRefs, nNodes, ref)
    )
    val GCInitResponse = Await.result(f, waitTime)
    GCInitResponse match {
      case GlobalCoordinator.InitResponse(message) =>
        println(message)
      case _ =>
        println("Failed to initialize the global coordinator")
    }
  }

  /**
   * Block until the number of cluster members with status UP == nNodes
   *
   * @param domainListener
   */
  def blockUntilAllMembersUp(
                              domainListener: ActorSystem[ClusterEvent.ClusterDomainEvent],
                              nNodes: Int
                            ): Unit = {
    implicit val scheduler = domainListener.scheduler

    var flag = true
    while (flag) {
      val f: Future[ClusterMemberEventListener.nMembersUpResponse] = domainListener.ask(ref => {
        ClusterMemberEventListener.nMembersUp(ref)
      })
      val nMembersUpResponse = Await.result(f, waitTime)
      nMembersUpResponse match {
        case ClusterMemberEventListener.nMembersUpResponse(n) =>
          //          println(s"$n of ${nNodes - 1} nodes are up")
          flag = n != nNodes
        case _ =>
          flag = true
      }
      Thread.sleep(1000)
    }
  }

  def blockUntilGlobalCoordinatorRegistered(
                                             entityManager: ActorSystem[EntityManager.Command]
                                           ): GCRef = {
    implicit val scheduler = entityManager.scheduler
    var flag = true
    var gcRef: GCRef = null
    while (flag) {
      val f: Future[EntityManager.GCRefResponseFromReceptionist] = entityManager.ask(ref => {
        EntityManager.askGCRefFromReceptionist(ref)
      })
      val GCRefResponseFromReceptionist = Await.result(f, waitTime)

      GCRefResponseFromReceptionist match {
        case EntityManager.GCRefResponseFromReceptionist(listing) =>
          val set = listing.serviceInstances(GlobalCoordinatorKey)
          // the partitionCoordinator for this pid has been registered
          if (set.size == 1) {
            gcRef = set.head
            flag = false
          }
      }
      Thread.sleep(1000)
    }

    val actorRefResolver = ActorRefResolver(entityManager)
    val serializedActorRef: Array[Byte]= actorRefResolver.toSerializationFormat(gcRef).getBytes(StandardCharsets.UTF_8)

    val str = new String(serializedActorRef, StandardCharsets.UTF_8)
    println("Serialized global coordinator actor ref", str)

    val deserializedActorRef = actorRefResolver.resolveActorRef[GCRef](str)
    println("heres the deserialized", deserializedActorRef)
    gcRef
  }

  /**
   * Block until the number of registered PartitionCoordinators == numberOfShards
   *
   * @param entityManager
   */
  def blockUntilAllPCsRegistered(
                                  entityManager: ActorSystem[EntityManager.Command],
                                  numberOfShards: Int
                                ): collection.mutable.Map[Int, PCRef] = {
    implicit val scheduler = entityManager.scheduler

    // build a map from partition id to PartitionCoordinator entity ref
    val pcRefs = collection.mutable.Map[Int, PCRef]()
    var flag = true

    while (flag) {
      var nRegisteredPCs = 0
      for (pid <- 0 until numberOfShards) {
        val PartitionCoordinatorKey = ServiceKey[PartitionCoordinator.Command](s"partitionCoordinator${pid}")
        val f: Future[EntityManager.PCRefResponseFromReceptionist] = entityManager.ask(ref => {
          EntityManager.askPCRefFromReceptionist(pid, ref)
        })
        val PCRefResponseFromReceptionist = Await.result(f, waitTime)

        PCRefResponseFromReceptionist match {
          case EntityManager.PCRefResponseFromReceptionist(listing) =>
            val set = listing.serviceInstances(PartitionCoordinatorKey)
            // the partitionCoordinator for this pid has been registered
            if (set.size == 1) {
              nRegisteredPCs += 1
              pcRefs(pid) = set.head
            }
        }
      }
      // all partition coordinators have been registered and are available through the cluster receptionist
      if (nRegisteredPCs == numberOfShards) {
        flag = false
      }
      Thread.sleep(1000)
    }
    assert(pcRefs.size == numberOfShards)
    pcRefs
  }

  /**
   * A call to initialize a main vertex. Blocks (Await.result) until the main vertex is initialized
   *
   * @param mainERef
   * @param eid
   * @param neighbors
   * @param mirrors
   */
  def blockInitMain(
                     mainERef: EntityRef[MainEntity.Initialize],
                     eid: EntityId,
                     neighbors: ArrayBuffer[EntityId],
                     mirrors: ArrayBuffer[EntityId],
                     totalMainsInitialized: Int
                   ): Int = {
    // async call to initialize main
    val future: Future[MainEntity.InitResponse] = mainERef.ask(ref =>
      MainEntity.Initialize(
        eid.vertexId,
        eid.partitionId,
        neighbors,
        mirrors,
        ref
      ))
    // blocking to wait until main vertex is initialized
    val mainInitResult = Await.result(future, waitTime)
    mainInitResult match {
      case MainEntity.InitResponse(_) =>
        totalMainsInitialized + 1
      case _ =>
        println(s"Failed to Initialize Main ${eid.vertexId}_${eid.partitionId}")
        0
    }
  }

  /**
   * A call to initialize a mirror vertex. Blocks (Await.result) until the mirror vertex is initialized
   *
   * @param mirrorERef
   * @param m
   * @param eid
   */
  def blockInitMirror(
                       mirrorERef: EntityRef[VertexEntity.Command],
                       m: EntityId,
                       eid: EntityId,
                       totalMirrorsInitialized: Int
                     ): Int = {
    val future: Future[MirrorEntity.InitResponse] = mirrorERef.ask(ref =>
      MirrorEntity.InitializeMirror(
        m.vertexId,
        m.partitionId,
        eid,
        ref
      )
    )
    // blocking to wait until mirror vertex is initialized
    val mirrorInitResult = Await.result(future, waitTime)
    mirrorInitResult match {
      case MirrorEntity.InitResponse(_) =>
        totalMirrorsInitialized + 1
      case _ =>
        println(s"Failed to Initialize Main ${eid.vertexId}_${eid.partitionId}")
        0
    }
  }

  def broadcastGCtoPCs(gcRef: GCRef, s: Scheduler): Unit = {
    implicit val scheduler = s
    val f: Future[GlobalCoordinator.BroadcastRefResponse] = gcRef.ask(ref => {
      GlobalCoordinator.BroadcastRef(gcRef, ref)
    })
    val BroadcastRefResponse = Await.result(f, waitTime)

    BroadcastRefResponse match {
      case GlobalCoordinator.BroadcastRefResponse(message) =>
        println(message)
    }

  }
}

