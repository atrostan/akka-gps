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

class MirrorEntity(
    ctx: ActorContext[VertexEntity.Command],
    nodeAddress: String,
    entityContext: EntityContext[VertexEntity.Command]
) extends AbstractBehavior[VertexEntity.Command](ctx)
    with VertexEntity {
  import MirrorEntity._

  var vertexId = 0
  var partitionId = 0
  private var main: EntityId = null
  // TODO mirrorCounter, neighbourCounter, summedTotal

  var value = 0 // Counter TEST ONLY

  // In order for vertices to be able to send messages, they need to sharding.entityRefFor by entity id
  val sharding = ClusterSharding(ctx.system)

  def ctxLog(event: String) {
    ctx.log.info(
      s"******************{} ${event} at {}, eid: {}",
      ctx.self.path,
      nodeAddress,
      entityContext.entityId
    )
  }

  override def onMessage(
      msg: VertexEntity.Command
  ): Behavior[VertexEntity.Command] = {
    msg match {
      case InitializeMirror(vid, pid, m) =>
        ctxLog("Initializing mirror")

        vertexId = vid
        partitionId = pid.toShort
        main = m
        Behaviors.same

      case VertexEntity.Begin =>
        ctxLog("Beginning compute")
        value += 1
        Behaviors.same

      case VertexEntity.Idle =>
        entityContext.shard ! ClusterSharding.Passivate(ctx.self)
        Behaviors.same
      case VertexEntity.StopVertex =>
        Behaviors.stopped(() => ctxLog("stopping ... passivated for idling"))

      // Counter actions TESTING ONLY
      case VertexEntity.Increment =>
        ctxLog("adding")
        value += 1
        Behaviors.same
      case VertexEntity.GetValue(replyTo) =>
        ctxLog("get value")
        replyTo ! VertexEntity.SubTtl(entityContext.entityId, value)
        Behaviors.same
      case VertexEntity.EchoValue =>
        ctxLog("echo (logging only) value")
        Behaviors.same

      case _ =>
        ctxLog("Unknown behaviour for mirror " + msg.toString)
        Behaviors.same

    }
  }
}
object MirrorEntity {
  val TypeKey: EntityTypeKey[VertexEntity.Command] =
    EntityTypeKey[VertexEntity.Command]("MirrorEntity")

  // Orchestration
  final case class InitializeMirror(
      vertexId: Int,
      partitionId: Int,
      main: EntityId
  ) extends VertexEntity.Command

  // GAS
  final case class NeighbourMessage(stepNum: Int, msg: String) extends VertexEntity.Command
  final case class ApplyResult(stepNum: Int, msg: String) extends VertexEntity.Command

  def apply(
      nodeAddress: String,
      entityContext: EntityContext[VertexEntity.Command]
  ): Behavior[VertexEntity.Command] = {
    Behaviors.setup(ctx => {
      ctx.setReceiveTimeout(30.seconds, VertexEntity.Idle)
      new MirrorEntity(ctx, nodeAddress, entityContext)
    })
  }
}