package com.cluster.graph.entity

import scala.concurrent.duration._
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{Behaviors, AbstractBehavior, ActorContext}
import akka.cluster.sharding.typed.scaladsl.{
  ClusterSharding,
  EntityContext,
  EntityTypeKey,
  EntityRef
}
import com.CborSerializable
import scala.collection.mutable.ArrayBuffer

trait VertexEntity {
  var vertexId: Int
  var partitionId: Short
  var value: Int
}
object VertexEntity {
  trait Command extends CborSerializable
  trait Response extends CborSerializable

  case object StopVertex extends Command
  case object Idle extends Command

  val TypeKey = EntityTypeKey[VertexEntity.Command]("VertexEntity")

  // GAS General Commands
  case object Begin extends VertexEntity.Command
  case object End extends VertexEntity.Command
  final case class NeighbourMessage(stepNum: Int, msg: String) extends VertexEntity.Command

  // Counter actions TESTING ONLY
  case object Increment extends VertexEntity.Command
  final case class GetValue(replyTo: ActorRef[VertexEntity.Response]) extends VertexEntity.Command
  case object EchoValue extends VertexEntity.Command
  case class SubTtl(entityId: String, ttl: Int) extends VertexEntity.Response

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
    if (getEntityClass(entityContext.entityId) == MAIN_ENTITY)
      Behaviors.setup(ctx => new MainEntity(ctx, nodeAddress, entityContext))
    else
      Behaviors.setup(ctx => new MirrorEntity(ctx, nodeAddress, entityContext))
  }
}
