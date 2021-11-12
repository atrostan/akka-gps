package com.cluster.graph.entity

import scala.concurrent.duration._
import akka.actor.typed.{Behavior}
import akka.actor.typed.scaladsl.{Behaviors, AbstractBehavior, ActorContext}
import akka.cluster.sharding.typed.scaladsl.{
  ClusterSharding,
  EntityContext,
  EntityTypeKey,
}

import VertexEntity._

class MirrorEntity(
    ctx: ActorContext[VertexEntity.Command],
    nodeAddress: String,
    entityContext: EntityContext[VertexEntity.Command]
) extends AbstractBehavior[VertexEntity.Command](ctx)
    with VertexEntity {
  import MirrorEntity._

  private var main: EntityId = null

  var value = 0 // Counter TEST ONLY

  // In order for vertices to be able to send messages, they need to sharding.entityRefFor by entity id
  val sharding = ClusterSharding(ctx.system)

  override def ctxLog(event: String): Unit = {
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

      // GAS Actions
      case VertexEntity.Begin(n) =>
        ctxLog("Beginning compute")
        value += 1
        Behaviors.same
      case VertexEntity.End =>
        ctxLog("Ordered to stop " + msg)
        // TODO Implement
        Behaviors.same

      case c: VertexEntity.NeighbourMessage => reactToNeighbourMessage(c)

      case ApplyResult(stepNum, oldVal, newVal) => {
        ctxLog("Received apply value from Main " + newVal)
        // TODO Implement
        Behaviors.same
      }

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

  override def applyIfReady(stepNum: SuperStep): Unit = {
    if(neighbourCounter(stepNum) == partitionInDegree) {
      val cmd = MirrorTotal(stepNum, summedTotal.get(stepNum))
      // TODO send cmd to main
    }
  }
}

object MirrorEntity {
  val TypeKey: EntityTypeKey[VertexEntity.Command] =
    EntityTypeKey[VertexEntity.Command]("MirrorEntity")

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
