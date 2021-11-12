package com.cluster.graph

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import com.typesafe.config.ConfigFactory
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.typed.Cluster
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityRef}
import akka.util.Timeout
import com.CborSerializable
import com.preprocessing.partitioning.oneDim.Main

import scala.collection.mutable.ArrayBuffer
import com.cluster.graph.entity._

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

// EntityManager actor
// in charge of both:
// (1) front end actions (incrementing counters),
// (2) managing and redirecting actions to respective actors and entities
object EntityManager {

  trait Command extends CborSerializable

  trait Response extends CborSerializable

  // Main/Mirror Initialization
  case class Initialize(
                         entityClass: String,
                         vertexId: Int,
                         partitionId: Int,
                         neighbors: ArrayBuffer[EntityId]
                       ) extends Command


  case class GetNMainsInitialized(replyTo: ActorRef[NMainsInitResponse]) extends Command
  case class NMainsInitResponse(n: Int)
  case class GetNMirrorsInitialized(replyTo: ActorRef[NMirrorsInitResponse]) extends Command
  case class NMirrorsInitResponse(n: Int)
  case class findPC(pid: Int) extends Command
  private case class ListingResponse(listing: Receptionist.Listing) extends Command


  // GAS
  case class TerminationVote(stepNum: Int) extends Command

  // Counter TEST ONLY
  case class AddOne(entityClass: String, vertexId: Int, partitionId: Int) extends Command

  case class GetSum(entityClass: String, vertexId: Int, partitionId: Int) extends Command

  case class WrappedTotal(res: VertexEntity.Response) extends Command

  case class Received(i: Int) extends Command


  def apply(
             partitionMap: collection.mutable.Map[Int, Int],
             mainArray: Array[Main]
           ): Behavior[Command] = Behaviors.setup { ctx =>

    val listingResponseAdapter = ctx.messageAdapter[Receptionist.Listing](ListingResponse.apply)

    val myShardAllocationStrategy = new MyShardAllocationStrategy(partitionMap)
    val cluster = Cluster(ctx.system)
    val sharding = ClusterSharding(ctx.system)

    var totalMainsInitialized = 0
    var totalMirrorsInitialized = 0

    val waitTime = 20 seconds
    implicit val timeout: Timeout = waitTime
    implicit val ec = ctx.system.executionContext

    /**
     * A call to initialize a main vertex. Blocks (Await.result) until the main vertex is initialized
     * @param mainERef
     * @param eid
     * @param neighbors
     * @param mirrors
     */
    def blockInitMain(
                       mainERef: EntityRef[MainEntity.Initialize],
                       eid: EntityId,
                       neighbors: ArrayBuffer[EntityId],
                       mirrors: ArrayBuffer[EntityId]
                     ): Unit = {
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
          totalMainsInitialized += 1
        case _ =>
          println(s"Failed to Initialize Main ${eid.vertexId}_${eid.partitionId}")
      }
    }

    /**
     * A call to initialize a mirror vertex. Blocks (Await.result) until the mirror vertex is initialized
     * @param mirrorERef
     * @param m
     * @param eid
     */
    def blockInitMirror(
                         mirrorERef: EntityRef[VertexEntity.Command],
                         m: EntityId,
                         eid: EntityId
                       ): Unit = {
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
          totalMirrorsInitialized += 1
        case _ =>
          println(s"Failed to Initialize Main ${eid.vertexId}_${eid.partitionId}")
      }
    }

    val numberOfShards = ConfigFactory.load("cluster")
    val messageExtractor = new VertexIdExtractor[VertexEntity.Command](
      numberOfShards.getInt("akka.cluster.sharding.number-of-shards")
    )

    val entityType = Entity(VertexEntity.TypeKey) { entityContext =>
      VertexEntity(cluster.selfMember.address.toString, entityContext)
    }
      // the shard allocation decisions are taken by the central ShardCoordinator, which is running as a cluster
      // singleton, i.e. one instance on the oldest member among all cluster nodes or a group of nodes tagged with a
      // specific role.
      .withRole("shard")
      .withStopMessage(VertexEntity.StopVertex)
      .withAllocationStrategy(myShardAllocationStrategy)
      .withMessageExtractor(messageExtractor)

    val shardRegion = sharding.init(entityType)
    val counterRef: ActorRef[VertexEntity.Response] =
      ctx.messageAdapter(ref => WrappedTotal(ref))

    def isMain(eid: EntityId): Boolean = {
      eid.vertexId % partitionMap.size == eid.partitionId
    }

    // Initialize vertex. If the vertex is a main, tell the command to all its mirrors.
    // TODO; should be asynchronous; before beginning computation, we must ensure all main and mirror
    // are initialized

    def initMainAndMirrors(
                            eid: EntityId,
                            neighbors: ArrayBuffer[EntityId]
                          ): Unit = {
      val entityRef: EntityRef[VertexEntity.Command] = sharding.entityRefFor(VertexEntity.TypeKey, eid.toString)
      // initialize all mirrors of main // TODO Review main check is needed anymore
      if (isMain(eid)) {
        val mainERef: EntityRef[MainEntity.Initialize] = sharding.entityRefFor(VertexEntity.TypeKey, eid.toString)
        val mirrors = mainArray(eid.vertexId).mirrors.map(m =>
          new EntityId(MirrorEntity.getClass.toString(), m.id, m.partition.id)
        )
        blockInitMain(mainERef, eid, neighbors, mirrors)
        for (m <- mirrors) {
          val mirrorERef: EntityRef[VertexEntity.Command] = sharding.entityRefFor(VertexEntity.TypeKey, m.toString)
          blockInitMirror(mirrorERef, m, eid)
        }
      }
    }

    // Tell non-parameter vertex command to a vertex.
    // If the vertex is a main, tell the command to all its mirrors.
    def tellMainAndMirrors(cmd: VertexEntity.Command, eid: EntityId): Unit = {
      val entityRef: EntityRef[VertexEntity.Command] =
        sharding.entityRefFor(VertexEntity.TypeKey, eid.toString)
      entityRef ! cmd
      if (isMain(eid)) {
        val mirrors = mainArray(eid.vertexId).mirrors.map(m =>
          new EntityId(MirrorEntity.getClass.toString(), m.id, m.partition.id)
        )
        val mirrorEntityRefs =
          mirrors.map(mid => sharding.entityRefFor(VertexEntity.TypeKey, mid.toString))
        for (eRef <- mirrorEntityRefs) eRef ! cmd
      }
    }

    Behaviors.receiveMessage[Command] {

      case Initialize(eCl, vid, pid, neighbors) =>

        val eid = new EntityId(eCl, vid, pid)
        println(eid)
        initMainAndMirrors(eid, neighbors)
        Behaviors.same

      case findPC(pid) =>
        val PartitionCoordinatorKey = ServiceKey[PartitionCoordinator.Command](s"partitionCoordinator${pid}")
        val f: Future[Receptionist.Listing] = ctx.system.receptionist.ask { replyTo =>
          Receptionist.Find(PartitionCoordinatorKey, replyTo)
        }

        val PCListingResult = Await.result(f, waitTime)
        PCListingResult match {
          case ActorRef[Receptionist.Listing] =>
            println(message)
            1
          case _ =>
            println(s"Failed to find PC thru Receptionist")
            0
        }
//        ctx.system.receptionist ! Receptionist.Find(PartitionCoordinatorKey, listingResponseAdapter)
        Behaviors.same

//      case ListingResponse(ServiceKey[PartitionCoordinator.Command](s"partitionCoordinator${pid}").Listing(listings)) =>
//        println("these are the listings,,,")
//        println(listings)
//        Behaviors.same

      case AddOne(eCl, vid, pid) =>
        val eid = new EntityId(eCl, vid, pid)
        // increment all mirrors of myself
        tellMainAndMirrors(VertexEntity.Increment, eid)
        Behaviors.same
      case GetSum(eCl, vid, pid) =>
        val eid = new EntityId(eCl, vid, pid)
        val entityRef: EntityRef[VertexEntity.Command] =
          sharding.entityRefFor(VertexEntity.TypeKey, eid.toString)
        entityRef ! VertexEntity.GetValue(counterRef)
        Behaviors.same
      case Received(i) =>
        Behaviors.same
      case WrappedTotal(ttl) =>
        ttl match {
          case VertexEntity.SubTtl(eid, subttl) =>
            ctx.log.info("***********************{} total: {} ", eid, subttl)
        }
        Behaviors.same

      case GetNMainsInitialized(replyTo) =>
        replyTo ! NMainsInitResponse(totalMainsInitialized)
        Behaviors.same

      case GetNMirrorsInitialized(replyTo) =>
        replyTo ! NMirrorsInitResponse(totalMirrorsInitialized)
        Behaviors.same

      case _ =>
        ctx.log.info("Unknown behaviour for entity manager")
        Behaviors.same
    }
  }
}
