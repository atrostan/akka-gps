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
import com.preprocessing.partitioning.oneDim.Main
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
    mainArray: Array[Main],
    pid: Int,
    partitionInDegree: Array[collection.mutable.Map[Int, Int]],
    tmpmains: Array[(Int, Set[Int], List[(Int, Int, Int)], Int)],
    tmpmirrors: Array[(Int, Int, List[(Int, Int, Int)], Int)]
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
        println(s"Got init mains on EM${pid}")
        tmpmains.map {
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
        println(s"${pid} total mains initialized: ", totalMainsInitialized)
        Behaviors.same

      case InitializeMirrors =>
        println(s"Got init mirrors on EM${pid}")
        tmpmirrors.map {
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
        println(s"${pid} total mirrors initialized: ", totalMirrorsInitialized)
        Behaviors.same

      case Initialize(eCl, vid, pid, neighbors) =>
        val eid = new EntityId(eCl, vid, pid)
        initMainAndMirrors(eid, neighbors)
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

  def spawnPartitionCoordinator(pid: Int): ActorRef[PartitionCoordinator.Command] = {
//    val mains = mainArray

//      .filter(m => m.partition.id == pid)
//      .map(m => new EntityId("Main", m.id, pid))
//      .toList
    val mains = tmpmains.map(m => new EntityId("Main", m._1, pid)).toList
    val pcChild = ctx.spawn(
      Behaviors.supervise(PartitionCoordinator(mains, pid)).onFailure(SupervisorStrategy.restart),
      name = s"pc$pid"
    )
    blockInitPartitionCoordinator(pcChild, mains, pid, scheduler)
    pcChild
  }

  def spawnGlobalCoordinator(): ActorRef[GlobalCoordinator.Command] = {
    val gcChild = ctx.spawn(
      Behaviors.supervise(GlobalCoordinator()).onFailure(SupervisorStrategy.restart),
      name = s"gc"
    )
    gcChild
  }

  // Initialize vertex. If the vertex is a main, tell the command to all its mirrors.
  // TODO; should be asynchronous; before beginning computation, we must ensure all main and mirror
  // are initialized
  def initMainAndMirrors(
      eid: EntityId,
      neighbors: ArrayBuffer[EntityId]
  ): Unit = {
    val entityRef: EntityRef[VertexEntity.Command] =
      sharding.entityRefFor(VertexEntity.TypeKey, eid.toString)
    // initialize all mirrors of main // TODO Review main check is needed anymore
    if (isMain(eid)) {
      val mainERef: EntityRef[VertexEntity.Initialize] =
        sharding.entityRefFor(VertexEntity.TypeKey, eid.toString)
      val mirrors = mainArray(eid.vertexId).mirrors.map(m =>
        new EntityId(VertexEntityType.Mirror.toString(), m.id, m.partition.id)
      )
      // TODO Pass partitionInDegree to all vertices being created
      val nInEdges = partitionInDegree(eid.partitionId)(eid.vertexId)
//      println("eid main, ", eid.partitionId, eid.vertexId, nInEdges)
//      totalMainsInitialized =
//        blockInitMain(mainERef, eid, neighbors, mirrors, nInEdges, totalMainsInitialized)
      for (m <- mirrors) {
        val mirrorERef: EntityRef[VertexEntity.Command] =
          sharding.entityRefFor(VertexEntity.TypeKey, m.toString)
        // TODO Need to add neighbours
        val neighbors = ArrayBuffer[EntityId]()
        val nInEdges = partitionInDegree(m.partitionId)(m.vertexId)
//        println("eid mirror, ", m.partitionId, m.vertexId, nInEdges)

//        totalMirrorsInitialized =
//          blockInitMirror(mirrorERef, m, eid, neighbors, nInEdges, totalMirrorsInitialized)
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
        new EntityId(VertexEntityType.Mirror.toString(), m.id, m.partition.id)
      )
      val mirrorEntityRefs =
        mirrors.map(mid => sharding.entityRefFor(VertexEntity.TypeKey, mid.toString))
      for (eRef <- mirrorEntityRefs) eRef ! cmd
    }
  }

  def isMain(eid: EntityId): Boolean = {
    eid.vertexId % partitionMap.size == eid.partitionId
  }
}

object EntityManager {

  def apply(
      partitionMap: collection.mutable.Map[Int, Int],
      mainArray: Array[Main],
      pid: Int,
      partitionInDegree: Array[collection.mutable.Map[Int, Int]],
      tmpmains: Array[(Int, Set[Int], List[(Int, Int, Int)], Int)],
      tmpmirrors: Array[(Int, Int, List[(Int, Int, Int)], Int)]
  ): Behavior[EntityManager.Command] = Behaviors.setup(ctx => {
    val EntityManagerKey =
      ServiceKey[EntityManager.Command](s"entityManager${pid}")
    ctx.system.receptionist ! Receptionist.Register(EntityManagerKey, ctx.self)
    new EntityManager(
      ctx,
      partitionMap,
      mainArray,
      pid,
      partitionInDegree,
      tmpmains,
      tmpmirrors
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
