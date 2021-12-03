package com.cluster.graph

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, Scheduler, SupervisorStrategy}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityRef}
import akka.cluster.typed.Cluster
import akka.util.Timeout
import com.CborSerializable
import com.cluster.graph.Init.{blockInitMain, blockInitMirror, blockInitPartitionCoordinator}
import com.cluster.graph.entity._
import com.typesafe.config.ConfigFactory

import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration._

// EntityManager actor
// in charge of both:
// (1) front end actions (incrementing counters),
// (2) managing and redirecting actions to respective actors and entities

class EntityManager(
    ctx: ActorContext[EntityManager.Command],
    partitionMap: collection.mutable.Map[Int, Int],
    pid: Int,
    mains: Array[(Int, Set[Int], List[(Int, Int, Int)], Int)],
    mirrors: Array[(Int, Int, List[(Int, Int, Int)], Int)]
) extends AbstractBehavior[EntityManager.Command](ctx) {

  import EntityManager._

  val listingResponseAdapter = ctx.messageAdapter[Receptionist.Listing](ListingResponse.apply)
  implicit val scheduler: Scheduler = ctx.system.scheduler

  val myShardAllocationStrategy = new MyShardAllocationStrategy(partitionMap)
  val cluster = Cluster(ctx.system)
  val sharding = ClusterSharding(ctx.system)
  val waitTime = 20 seconds
  val numberOfShards = ConfigFactory.load("cluster")
  val messageExtractor = new VertexIdExtractor[VertexEntity.Command](
    numberOfShards.getInt("akka.cluster.sharding.number-of-shards")
  )
  implicit val timeout: Timeout = waitTime
  implicit val ec = ctx.system.executionContext
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
  var totalMainsInitialized = 0
  var totalMirrorsInitialized = 0
  private var partitionCoordinator: ActorRef[PartitionCoordinator.Command] = null
  private var globalCoordinator: ActorRef[GlobalCoordinator.Command] = null

  override def onMessage(msg: EntityManager.Command): Behavior[EntityManager.Command] = {
    msg match {
      case InitializeMains =>
        mains.map {
          case (vid, mirrorPids, neighs, partitionInDegree) =>
            val eid = new EntityId(VertexEntityType.Main.toString(), vid, pid)
            val mainERef: EntityRef[VertexEntity.Initialize] =
              sharding.entityRefFor(VertexEntity.TypeKey, eid.toString)
            val neighbors = neighs.map {
              case (dest, wt, tag) =>
                if (tag == 0) { // main -> main
                  (new EntityId(VertexEntityType.Main.toString(), dest, pid), wt)
                } else { // main -> mirror
                  (new EntityId(VertexEntityType.Mirror.toString(), dest, pid), wt)
                }
            }
            val mirrors = mirrorPids.map(pid =>
              new EntityId(VertexEntityType.Mirror.toString(), vid, pid)
            ).toList
            totalMainsInitialized = blockInitMain(mainERef, eid, neighbors, mirrors, partitionInDegree, totalMainsInitialized)
        }
        println(s"mains on pid ${pid}")
        mains.foreach(x => println(pid, x))
        println(s"${pid} total mains initialized: ", totalMainsInitialized)
        Behaviors.same

      case InitializeMirrors =>
        mirrors.map {
          case (vid, mainPid, neighs, partitionInDegree) =>
            val mid = new EntityId(VertexEntityType.Main.toString(), vid, mainPid)
            val eid = new EntityId(VertexEntityType.Mirror.toString(), vid, pid)
            val mirrorERef: EntityRef[VertexEntity.Command] =
              sharding.entityRefFor(VertexEntity.TypeKey, eid.toString)
            val neighbors = neighs.map {
              case (dest, wt, tag) =>
                if (tag == 2) { // mirror -> main
                  (new EntityId(VertexEntityType.Main.toString(), dest, pid), wt)
                } else { // mirror -> mirror
                  (new EntityId(VertexEntityType.Mirror.toString(), dest, pid), wt)
                }
            }
            totalMirrorsInitialized = blockInitMirror(mirrorERef, eid, mid, neighbors,  partitionInDegree, totalMirrorsInitialized)
        }
        println(s"mirrors on pid ${pid}")
        mirrors.foreach(x => println(pid, x))
        println(s"${pid} total mirrors initialized: ", totalMirrorsInitialized)
        Behaviors.same

      case SpawnPC(pid) =>
        partitionCoordinator = spawnPartitionCoordinator(pid)
        Behaviors.same

      case SpawnGC() =>
        globalCoordinator = spawnGlobalCoordinator()
        Behaviors.same

      case AskRefFromReceptionist(serviceKey, replyTo) =>
        val f: Future[Receptionist.Listing] =
          ctx.system.receptionist.ask(replyTo => Receptionist.Find(serviceKey, replyTo))
        val result = Await.result(f, waitTime)
        replyTo ! RefResponseFromReceptionist(result)
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

  def spawnPartitionCoordinator(pid: Int): ActorRef[PartitionCoordinator.Command] = {
    val ms = mains.map(m => new EntityId("Main", m._1, pid)).toList
    val pcChild = ctx.spawn(
      Behaviors.supervise(PartitionCoordinator(ms, pid)).onFailure(SupervisorStrategy.restart),
      name = s"pc$pid"
    )
    blockInitPartitionCoordinator(pcChild, ms, pid, scheduler)
    pcChild
  }

  def spawnGlobalCoordinator(): ActorRef[GlobalCoordinator.Command] = {
    val gcChild = ctx.spawn(
      Behaviors.supervise(GlobalCoordinator()).onFailure(SupervisorStrategy.restart),
      name = s"gc"
    )
    gcChild
  }
}

object EntityManager {

  def apply(
      partitionMap: collection.mutable.Map[Int, Int],
      pid: Int,
      mains: Array[(Int, Set[Int], List[(Int, Int, Int)], Int)],
      mirrors: Array[(Int, Int, List[(Int, Int, Int)], Int)]
  ): Behavior[EntityManager.Command] = Behaviors.setup(ctx => {
    val EntityManagerKey =
      ServiceKey[EntityManager.Command](s"entityManager${pid}")
    ctx.system.receptionist ! Receptionist.Register(EntityManagerKey, ctx.self)
    new EntityManager(
      ctx,
      partitionMap,
      pid,
      mains,
      mirrors
    )
  })
  // command/response typedef
  sealed trait Command extends CborSerializable
  sealed trait Response extends CborSerializable

  // Sync Main/Mirror Initialization
  final case object InitializeMains extends Command
  final case object InitializeMirrors extends Command

  final case class Initialize(
      entityClass: String,
      vertexId: Int,
      partitionId: Int,
      neighbors: ArrayBuffer[EntityId]
  ) extends Command

  // Init ASync Command
  final case class SpawnPC(pid: Int) extends Command
  final case class SpawnGC() extends Command

  // Init Sync Command
  final case class GetNMainsInitialized(replyTo: ActorRef[NMainsInitResponse]) extends Command
  final case class GetNMirrorsInitialized(replyTo: ActorRef[NMirrorsInitResponse]) extends Command

  // Init Sync Receptionist Query
  final case class AskRefFromReceptionist[T](
      sk: ServiceKey[T],
      replyTo: ActorRef[RefResponseFromReceptionist]
  ) extends Command

  // Init Sync Response
  final case class NMainsInitResponse(n: Int) extends Response
  final case class NMirrorsInitResponse(n: Int) extends Response

  // Init Sync Receptionist Response
  final case class RefResponseFromReceptionist(listing: Receptionist.Listing) extends Response

  // GAS
  final case class TerminationVote(stepNum: Int) extends Command

  // Counter TEST ONLY
  final case class AddOne(entityClass: String, vertexId: Int, partitionId: Int) extends Command
  final case class GetSum(entityClass: String, vertexId: Int, partitionId: Int) extends Command
  final case class WrappedTotal(res: VertexEntity.Response) extends Command
  final case class Received(i: Int) extends Command

  private case class ListingResponse(listing: Receptionist.Listing) extends Command
}
