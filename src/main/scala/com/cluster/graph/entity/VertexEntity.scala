package com.cluster.graph.entity

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.scaladsl.{EntityContext, EntityTypeKey}
import com.CborSerializable

trait VertexEntity {
  var vertexId: Int
  var partitionId: Short
  var value: Int
}
object VertexEntity {
  val TypeKey = EntityTypeKey[VertexEntity.Command]("VertexEntity")
  val MAIN_ENTITY = MainEntity.getClass().toString()
  val MIRROR_ENTITY = MirrorEntity.getClass().toString()

  def getEntityClass(entityId: String): String = {
    entityId.split("_").head
  }

  def apply(
      nodeAddress: String,
      entityContext: EntityContext[VertexEntity.Command]
  ): Behavior[VertexEntity.Command] = {
    // TODO HACK to enable polymorphism to work together with sharding entityType. Otherwise shard will only use single type
    VertexEntityType.withName(EntityId.getTypeFromString(entityContext.entityId)) match {
      case VertexEntityType.Main =>
        Behaviors.setup(ctx => new MainEntity(ctx, nodeAddress, entityContext))
      case VertexEntityType.Mirror =>
        Behaviors.setup(ctx => new MirrorEntity(ctx, nodeAddress, entityContext))
    }
  }
  // command/response typedef
  trait Command extends CborSerializable
  trait Response extends CborSerializable

  // PartitionCoordinator Commands
  final case class NotifyLocation(replyTo: ActorRef[LocationResponse]) extends VertexEntity.Command

  // GAS General Commands
  final case object Begin extends VertexEntity.Command
  final case object End extends VertexEntity.Command
  final case class NeighbourMessage(stepNum: Int, msg: String) extends VertexEntity.Command
  final case object StopVertex extends Command
  final case object Idle extends Command

  // Counter actions TESTING ONLY
  final case object Increment extends VertexEntity.Command
  final case object EchoValue extends VertexEntity.Command
  final case class GetValue(replyTo: ActorRef[VertexEntity.Response]) extends VertexEntity.Command

  final case class LocationResponse(message: String) extends Response
  final case class SubTtl(entityId: String, ttl: Int) extends VertexEntity.Response

}

// Types of VertexEntities available in shard // TODO Part of HACK, extra coupling
object VertexEntityType extends Enumeration {
  type VertexEntityType = Value
  val Main = Value("Main")
  val Mirror = Value("Mirror")
}
