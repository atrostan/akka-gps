package com.cluster.graph

import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.{ActorRef, ActorRefResolver, ActorSystem, Scheduler}
import akka.cluster.ClusterEvent
import akka.cluster.sharding.typed.scaladsl.EntityRef
import akka.util.Timeout
import com.Typedefs.{EMRef, GCRef, PCRef}
import com.cluster.graph
import com.cluster.graph.GlobalCoordinator.GlobalCoordinatorKey
import com.cluster.graph.entity.{EntityId, MainEntity, MirrorEntity, VertexEntity}
import com.graph.{Edge, Vertex}
import com.preprocessing.partitioning.oneDim.Partitioning

import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

object Init {
  val waitTime = 10 seconds
  implicit val timeout: Timeout = waitTime

  // Sample graph for partitioning and akka population test
  def initGraphPartitioning(nPartitions: Int): Partitioning = {
    val edges = ArrayBuffer[Edge]()
    val nNodes: Int = 8
    val nEdges: Int = 20

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

    val e14 = Edge(v1, v0)
    val e15 = Edge(v2, v1)
    val e16 = Edge(v3, v2)
    val e17 = Edge(v1, v3)
    val e18 = Edge(v6, v5)
    val e19 = Edge(v7, v6)

    val vs = ArrayBuffer(v0, v1, v2, v3, v4)

    val es = ArrayBuffer(e0, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19)
    // create partitioning data structure
    val png = Partitioning(nPartitions, es, nNodes, nEdges)
    println("Partitioning result:")
    println(png)
    png
  }

  def blockInitPartitionCoordinator(
      pc: ActorRef[PartitionCoordinator.Command],
      mains: List[EntityId],
      partitionId: Int,
      s: Scheduler
  ): Int = {
    implicit val scheduler = s
    val future: Future[PartitionCoordinator.InitializeResponse] = pc.ask(ref =>
      PartitionCoordinator.Initialize(
        mains,
        partitionId,
        ref
      )
    )
    val pcInitResult = Await.result(future, waitTime)
    pcInitResult match {
      case PartitionCoordinator.InitializeResponse(message) =>
        println(message)
        1
      case _ =>
        println(s"Failed to init PartitionCoordinator for partition ${partitionId}")
        0
    }
  }

  def getNMainsAckd(
      entityManager: ActorSystem[EntityManager.Command],
      pcRef: PCRef
  ): Int = {
    implicit val scheduler = entityManager.scheduler
    val future: Future[PartitionCoordinator.NMainsAckdResponse] =
      pcRef.ask(ref => PartitionCoordinator.GetNMainsAckd(ref))
    val result = Await.result(future, waitTime)
    result match {
      case PartitionCoordinator.NMainsAckdResponse(totalMainsAckd) =>
        totalMainsAckd
      case _ =>
        println("Failed to get number of ackd mains")
        0
    }
  }

  def getNMainsInitialized(
      entityManager: ActorSystem[EntityManager.Command],
      emRef: EMRef
  ): Int = {
    // check that all mains have been correctly initialized
    implicit val scheduler = entityManager.scheduler
    val future: Future[EntityManager.NMainsInitResponse] =
      emRef.ask(ref => EntityManager.GetNMainsInitialized(ref))
    val result = Await.result(future, waitTime)
    result match {
      case EntityManager.NMainsInitResponse(totalMainsInitialized) =>
        totalMainsInitialized
      case _ =>
        println("Failed to get number of initialized mains")
        0
    }
  }

  def getNMirrorsInitialized(
      entityManager: ActorSystem[EntityManager.Command],
      emRef: EMRef,
  ): Int = {
    // check that all mains have been correctly initialized
    //    entityManager ! EntityManager.GetNMainsInitialized()

    implicit val scheduler = entityManager.scheduler
    val future: Future[EntityManager.NMirrorsInitResponse] =
      emRef.ask(ref => EntityManager.GetNMirrorsInitialized(ref))
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
      gc: GCRef,
      sched: Scheduler,
      pcRefs: collection.mutable.Map[Int, PCRef],
      nNodes: Int
  ): Unit = {
    implicit val scheduler = sched

    val f: Future[GlobalCoordinator.InitializeResponse] =
      gc.ask(ref => GlobalCoordinator.Initialize(pcRefs, nNodes, ref))
    val GCInitResponse = Await.result(f, waitTime)
    GCInitResponse match {
      case GlobalCoordinator.InitializeResponse(message) =>
        println(message)
      case _ =>
        println("Failed to initialize the global coordinator")
    }
  }

  /** Block until the number of cluster members with status UP == nNodes
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

//  def blockUntilGlobalCoordinatorRegistered(
//      entityManager: ActorSystem[EntityManager.Command]
//  ): GCRef = {
//    implicit val scheduler = entityManager.scheduler
//    var flag = true
//    var gcRef: GCRef = null
//    while (flag) {
//      val f: Future[EntityManager.GCRefResponseFromReceptionist] = entityManager.ask(ref => {
//        EntityManager.askGCRefFromReceptionist(ref)
//      })
//      val GCRefResponseFromReceptionist = Await.result(f, waitTime)
//
//      GCRefResponseFromReceptionist match {
//        case EntityManager.GCRefResponseFromReceptionist(listing) =>
//          val set = listing.serviceInstances(GlobalCoordinatorKey)
//          // the partitionCoordinator for this pid has been registered
//          if (set.size == 1) {
//            gcRef = set.head
//            flag = false
//          }
//      }
//      Thread.sleep(1000)
//    }
//
//    val actorRefResolver = ActorRefResolver(entityManager)
//    val serializedActorRef: Array[Byte] =
//      actorRefResolver.toSerializationFormat(gcRef).getBytes(StandardCharsets.UTF_8)
//
//    val str = new String(serializedActorRef, StandardCharsets.UTF_8)
//    println("Serialized global coordinator actor ref", str)
//
//    val deserializedActorRef = actorRefResolver.resolveActorRef[GCRef](str)
//    println("heres the deserialized", deserializedActorRef)
//    gcRef
//  }

  /**
   * Block execution of akka-gps until nToRegister members of type T registered with service key
   * ServiceKey[T](s"$idStr$pid") have been registered by the cluster receptionist
   *
   * @param entityManager the ActorSystem that communicates with the receptionist
   * @param idStr the name of service key id that is registered with the receptionist
   * @param nToRegister the number of expected registered service keys
   * @tparam T either {EntityManager, PartitionCoordinator, GlobalCoordinator}.Command
   * @return
   */
  def blockUntilAllRefsRegistered[T: ClassTag](
      entityManager: ActorSystem[EntityManager.Command],
      idStr: String,
      nToRegister: Int
  ): collection.mutable.Map[Int, ActorRef[T]] = {
    implicit val scheduler = entityManager.scheduler

    // build a map from partition id to entity ref
    val refs = collection.mutable.Map[Int, ActorRef[T]]()
    var flag = true

    while (flag) {
      var nRegistered = 0
      for (pid <- 0 until nToRegister) {

        var serviceKey: ServiceKey[T] = null
        if (nToRegister == 1) {
          serviceKey = ServiceKey[T](s"$idStr")
        } else {
          serviceKey = ServiceKey[T](s"$idStr$pid")
        }

        val f: Future[EntityManager.RefResponseFromReceptionist] = entityManager.ask(ref => {
          EntityManager.AskRefFromReceptionist(serviceKey, ref)
        })
        val RefResponseFromReceptionist = Await.result(f, waitTime)

        RefResponseFromReceptionist match {
          case EntityManager.RefResponseFromReceptionist(listing) =>
            val set = listing.serviceInstances(serviceKey)
            // the ref for this pid has been registered
            if (set.size == 1) {
              nRegistered += 1
              refs(pid) = set.head
            }
        }
      }
      // all refs have been registered and are available through the cluster receptionist
      if (nRegistered == nToRegister) {
        flag = false
      }
      Thread.sleep(1000)
    }
    assert(refs.size == nToRegister)
    refs
  }

  /** A call to initialize a main vertex. Blocks (Await.result) until the main vertex is initialized
    *
    * @param mainERef
    * @param eid
    * @param neighbors
    * @param mirrors
    */
  def blockInitMain(
      mainERef: EntityRef[VertexEntity.Initialize],
      eid: EntityId,
      neighbors: List[(EntityId, Int)],
      mirrors: List[EntityId],
      inDegree: Int,
      totalMainsInitialized: Int
  ): Int = {
    // async call to initialize main
    val future: Future[VertexEntity.InitializeResponse] = mainERef.ask(ref =>
      VertexEntity.Initialize(
        eid.vertexId,
        eid.partitionId,
        neighbors,
        mirrors,
        inDegree,
        ref
      )
    )
    // blocking to wait until main vertex is initialized
    val mainInitResult = Await.result(future, waitTime)
    mainInitResult match {
      case VertexEntity.InitializeResponse(_) =>
        // println(s"initialized main ${eid.vertexId} on partition ${eid.partitionId} with neighbors ${neighbors.sortBy(n => n._1.vertexId)} and ${inDegree} incoming neighbours")
        totalMainsInitialized + 1
      case _ =>
        println(s"Failed to Initialize Main ${eid.vertexId}_${eid.partitionId}")
        0
    }
  }

  /** A call to initialize a mirror vertex. Blocks (Await.result) until the mirror vertex is
    * initialized
    *
    * @param mirrorERef
    * @param m Entity id of the mirror vertex
    * @param eid Entity id of its main vertex
    * @param neighbors List of neighbours
    */
  def blockInitMirror(
      mirrorERef: EntityRef[VertexEntity.Command],
      m: EntityId,
      eid: EntityId,
      neighbors: List[(EntityId, Int)],
      inDegree: Int,
      totalMirrorsInitialized: Int
  ): Int = {
    val future: Future[VertexEntity.InitializeResponse] = mirrorERef.ask(ref =>
      VertexEntity.InitializeMirror(
        m.vertexId,
        m.partitionId,
        eid,
        neighbors,
        inDegree,
        ref
      )
    )
    // blocking to wait until mirror vertex is initialized
    val mirrorInitResult = Await.result(future, waitTime)
    mirrorInitResult match {
      case VertexEntity.InitializeResponse(_) =>
        // println(s"initialized mirror ${m.vertexId} on partition ${m.partitionId} with neighbors ${neighbors.sortBy(n => n._1.vertexId)} and ${inDegree} incoming neighbours")
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
