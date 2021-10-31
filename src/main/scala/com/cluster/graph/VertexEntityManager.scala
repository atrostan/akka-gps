package com.cluster.graph

import com.typesafe.config.ConfigFactory
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.typed.Cluster
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.HashCodeMessageExtractor
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ShardingMessageExtractor
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityRef}
import com.preprocessing.partitioning.oneDim.Main

import java.io.{File, FileOutputStream}
import scala.collection.mutable.ArrayBuffer

// EntityManager actor
// in charge of both:
// (1) front end actions (incrementing counters),
// (2) managing and redirecting actions to respective actors and entities
object VertexEntityManager {

  sealed trait Command

  case class Initialize(vertexId: String) extends Command

  case class AddOne(vertexId: String) extends Command
  
  case class GetSum(counterId: String) extends Command

  case class WrappedTotal(res: Vertex.Response) extends Command

  case class Received(i: Int) extends Command

  def apply(partitionMap: collection.mutable.Map[Int, Int], mainArray: Array[Main]): Behavior[Command] = Behaviors.setup { ctx =>
    val myShardAllocationStrategy = new MyShardAllocationStrategy(partitionMap)
    val cluster = Cluster(ctx.system)
    val sharding = ClusterSharding(ctx.system)

    val numberOfShards = ConfigFactory.load("cluster")
    val messageExtractor = new VertexIdExtractor[Vertex.Command](numberOfShards.getInt("akka.cluster.sharding.number-of-shards"))

    println("partitionMap in VertexEntityManager: ", partitionMap)
    println("main array in  ")
    mainArray.foreach(println)
    println("address: ", cluster.selfMember.address.toString)

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

    def isMain(eid: String): Boolean = {
      val split = eid.split('_').map(_.toInt)
      split(0) % partitionMap.size == split(1)
    }

    Behaviors.receiveMessage[Command] {
      case Initialize(cid) =>
        val entityRef: EntityRef[Vertex.Command] = sharding.entityRefFor(Vertex.TypeKey, cid)

        println(s"in init for ${cid}: isMain? ${isMain(cid)} ")
        if (isMain(cid)) {
          val vid = cid.split("_")(0).toInt
          val mirrors = mainArray(vid).mirrors
          mirrors.map(m => m.eid)
          entityRef ! Vertex.Initialize(mirrors.map(m => m.eid).toList)
        }
        entityRef ! Vertex.Initialize(List())

        Behaviors.same

      case AddOne(cid) =>
        val entityRef: EntityRef[Vertex.Command] = sharding.entityRefFor(Vertex.TypeKey, cid)
        entityRef ! Vertex.Increment
        Behaviors.same

      case GetSum(cid) =>
        val entityRef: EntityRef[Vertex.Command] = sharding.entityRefFor(Vertex.TypeKey, cid)
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
 * @param message The message to be send to the entity.
 * @throws `InvalidMessageException` if message is null.
 */
// final case class VertexShardingEnvelope[M](entityId: String, message: M) extends WrappedMessage {
//   if (message == null) throw InvalidMessageException("[null] is not an allowed message")
// }
