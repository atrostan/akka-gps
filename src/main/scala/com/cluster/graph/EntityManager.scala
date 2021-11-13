package com.cluster.graph

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.AskPattern.Askable
import com.typesafe.config.ConfigFactory
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.typed.Cluster
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityRef}
import akka.util.Timeout
import com.CborSerializable
import com.cluster.graph.GlobalCoordinator.GlobalCoordinatorKey
import com.cluster.graph.Init.{blockInitMain, blockInitMirror}
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
  case class NMainsInitResponse(n: Int) extends Response
  case class GetNMirrorsInitialized(replyTo: ActorRef[NMirrorsInitResponse]) extends Command
  case class NMirrorsInitResponse(n: Int) extends Response

  case class askPCRefFromReceptionist(pid: Int, replyTo: ActorRef[PCRefResponseFromReceptionist]) extends Command
  case class PCRefResponseFromReceptionist(listing: Receptionist.Listing) extends Response

  case class askGCRefFromReceptionist(replyTo: ActorRef[GCRefResponseFromReceptionist]) extends Command
  case class GCRefResponseFromReceptionist(listing: Receptionist.Listing) extends Response

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
    implicit val scheduler = ctx.system.scheduler

    var totalMainsInitialized = 0
    var totalMirrorsInitialized = 0

    val waitTime = 20 seconds
    implicit val timeout: Timeout = waitTime
    implicit val ec = ctx.system.executionContext



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
        totalMainsInitialized = blockInitMain(mainERef, eid, neighbors, mirrors, totalMainsInitialized)
        for (m <- mirrors) {
          val mirrorERef: EntityRef[VertexEntity.Command] = sharding.entityRefFor(VertexEntity.TypeKey, m.toString)
          totalMirrorsInitialized = blockInitMirror(mirrorERef, m, eid, totalMirrorsInitialized)
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
        initMainAndMirrors(eid, neighbors)
        Behaviors.same

      case askPCRefFromReceptionist(pid, replyTo) =>
        val PartitionCoordinatorKey = ServiceKey[PartitionCoordinator.Command](s"partitionCoordinator${pid}")
        val f: Future[Receptionist.Listing] = ctx.system.receptionist.ask(replyTo =>
          Receptionist.Find(PartitionCoordinatorKey, replyTo)
        )

        val PCListingResult = Await.result(f, waitTime)
        replyTo ! PCRefResponseFromReceptionist(PCListingResult)
        Behaviors.same

      case askGCRefFromReceptionist(replyTo) =>
        val f: Future[Receptionist.Listing] = ctx.system.receptionist.ask(replyTo =>
          Receptionist.Find(GlobalCoordinatorKey, replyTo)
        )

        val GCListingResult = Await.result(f, waitTime)
        replyTo ! GCRefResponseFromReceptionist(GCListingResult)
        Behaviors.same


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
