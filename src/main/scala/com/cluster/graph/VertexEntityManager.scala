package com.cluster.graph

import com.typesafe.config.ConfigFactory
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.typed.Cluster
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ShardingMessageExtractor
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityRef}
import com.preprocessing.partitioning.oneDim.Main
import scala.collection.mutable.ArrayBuffer

// EntityManager actor
// in charge of both:
// (1) front end actions (incrementing counters),
// (2) managing and redirecting actions to respective actors and entities
object VertexEntityManager {

  sealed trait Command

  case class Initialize(vertexId: Int, partitionId: Int, neighbors: ArrayBuffer[EntityId]) extends Command

  case class AddOne(vertexId: Int, partitionId: Int) extends Command

  case class GetSum(vertexId: Int, partitionId: Int) extends Command

  case class WrappedTotal(res: Vertex.Response) extends Command

  case class Received(i: Int) extends Command

  def apply(partitionMap: collection.mutable.Map[Int, Int], mainArray: Array[Main]): Behavior[Command] = Behaviors.setup { ctx =>
    val myShardAllocationStrategy = new MyShardAllocationStrategy(partitionMap)
    val cluster = Cluster(ctx.system)
    val sharding = ClusterSharding(ctx.system)

    val numberOfShards = ConfigFactory.load("cluster")
    val messageExtractor = new VertexIdExtractor[Vertex.Command](numberOfShards.getInt("akka.cluster.sharding.number-of-shards"))

    val entityType = Entity(Vertex.TypeKey) { entityContext =>
      Vertex(cluster.selfMember.address.toString, entityContext)
    }
      // the shard allocation decisions are taken by the central ShardCoordinator, which is running as a cluster
      // singleton, i.e. one instance on the oldest member among all cluster nodes or a group of nodes tagged with a
      // specific role.
      .withRole("shard")
      .withStopMessage(Vertex.StopVertex)
      .withAllocationStrategy(myShardAllocationStrategy)
      .withMessageExtractor(messageExtractor)

    val shardRegion = sharding.init(entityType)
    val counterRef: ActorRef[Vertex.Response] = ctx.messageAdapter(ref => WrappedTotal(ref))

    def isMain(eid: EntityId): Boolean = {
      eid.vertexId % partitionMap.size == eid.partitionId
    }

    // Initialize vertex. If the vertex is a main, tell the command to all its mirrors.
    def initMainAndMirrors(eid: EntityId, neighbors: ArrayBuffer[EntityId]): Unit = {
      val entityRef: EntityRef[Vertex.Command] = sharding.entityRefFor(Vertex.TypeKey, eid.toString)
      // initialize all mirrors of main // TODO Review main check is needed anymore
      if (isMain(eid)) {
        val mirrors = mainArray(eid.vertexId).mirrors.map(m => new EntityId(m.id, m.partition.id))
        entityRef ! Vertex.Initialize(eid.vertexId, eid.partitionId, neighbors, mirrors)
        // val mirrorEntityRefs = mirrors.map(mid => sharding.entityRefFor(Vertex.TypeKey, mid.toString))
        for (m <- mirrors) {
          val eRef = sharding.entityRefFor(Vertex.TypeKey, m.toString)
          eRef ! Vertex.InitializeMirror(m.vertexId, m.partitionId, eid)
        }
      }
    }

    // Tell non-parameter vertex command to a vertex.
    // If the vertex is a main, tell the command to all its mirrors.
    def tellMainAndMirrors(cmd: Vertex.Command, eid: EntityId): Unit = {
      val entityRef: EntityRef[Vertex.Command] = sharding.entityRefFor(Vertex.TypeKey, eid.toString)
      entityRef ! cmd
      // initialize all mirrors of main
      if (isMain(eid)) {
        val mirrors = mainArray(eid.vertexId).mirrors.map(m => new EntityId(m.id, m.partition.id))
        val mirrorEntityRefs = mirrors.map(mid => sharding.entityRefFor(Vertex.TypeKey, mid.toString))
        for (eRef <- mirrorEntityRefs) eRef ! cmd
      }
    }

    Behaviors.receiveMessage[Command] {
      case Initialize(vid, pid, neighbors) =>
        val eid = new EntityId(vid, pid)
        initMainAndMirrors(eid, neighbors)
        Behaviors.same
        
      case AddOne(vid, pid) =>
        val eid = new EntityId(vid, pid)
        tellMainAndMirrors(Vertex.Increment, eid)
        Behaviors.same

      case GetSum(vid, pid) =>
        val eid = new EntityId(vid, pid)
        val entityRef: EntityRef[Vertex.Command] = sharding.entityRefFor(Vertex.TypeKey, eid.toString)
        entityRef ! Vertex.GetValue(counterRef)
        Behaviors.same

      case Received(i) =>
        Behaviors.same

      case WrappedTotal(ttl) => ttl match {
        case Vertex.SubTtl(eid, subttl) =>
          ctx.log.info("***********************{} total: {} ", eid, subttl)
      }
        Behaviors.same
    }
  }
}

// TODO Review if this makes sense. Hard to find good examples. See ShardRegion and ShardingMessageExtractor classes.
// private final class VertexIdExtractor(shards: Int) extends HashCodeMessageExtractor(shards) {
//   override def entityId(message: Vertex.Command): String = s"${message.vertexId}_${message.partitionId}" // TODO Would be ideal to have this
//   override final def shardId(entityId: String): String = VertexIdExtractor.shardId(entityId, shards)
// }

// NOTE: With Envelope, may be better off without
final class VertexIdExtractor[M](val numberOfShards: Int)
  extends ShardingMessageExtractor[ShardingEnvelope[M], M] {

  override def entityId(envelope: ShardingEnvelope[M]): String = envelope.entityId // TODO Figure out how to do // VertexIdExtractor.shardId(envelope)

  override def shardId(entityId: String): String = VertexIdExtractor.shardId(entityId, numberOfShards)

  override def unwrapMessage(envelope: ShardingEnvelope[M]): M = envelope.message
}

object VertexIdExtractor {
  // private def entityId(envelope: ShardingEnvelope[M]): String = s"${envelope.vertexId}_${envelope.partitionId}"
  private def shardId(entityId: String, numberOfShards: Int): String = (entityId.split("_").tail.head.toInt % numberOfShards).toString
}

// TODO Use this to potentially be able to override entityId with custom structure above
/**
 * Re-implementation of envelope type that may be used with Cluster Sharding.
 *
 * Cluster Sharding provides a default [[HashCodeMessageExtractor]] that is able to handle
 * these types of messages, by hashing the entityId into into the shardId. It is not the only,
 * but a convenient way to send envelope-wrapped messages via cluster sharding.
 *
 * The alternative way of routing messages through sharding is to not use envelopes,
 * and have the message types themselves carry identifiers.
 *
 * @param entityId The business domain identifier of the entity.
 * @param message  The message to be send to the entity.
 * @throws `InvalidMessageException` if message is null.
 */
// final case class VertexShardingEnvelope[M](entityId: String, message: M) extends WrappedMessage {
//   if (message == null) throw InvalidMessageException("[null] is not an allowed message")
// }

// TODO Put in appropriate place
final class EntityId(vid: Int, pid: Int) {
  val vertexId = vid
  val partitionId = pid
  override def toString(): String = s"${vertexId}_${partitionId}"
}
