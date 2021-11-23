package com.cluster.graph.entity

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityContext, EntityTypeKey}
import com.CborSerializable

import scala.concurrent.duration._

import VertexEntity._
import com.algorithm.VertexInfo

class MirrorEntity(
    ctx: ActorContext[VertexEntity.Command],
    nodeAddress: String,
    entityContext: EntityContext[VertexEntity.Command]
) extends AbstractBehavior[VertexEntity.Command](ctx)
    with VertexEntity {

  private var main: EntityId = null

  var value = 0 // Counter TEST ONLY

  // In order for vertices find refs for messages, they need to sharding.entityRefFor by entity id
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
      case VertexEntity.InitializeMirror(vid, pid, m, neighs, inDeg, replyTo) =>
        vertexId = vid
        partitionId = pid.toShort
        neighbors = neighs
        main = m
        partitionInDegree = inDeg
        thisVertexInfo = VertexInfo(vertexId, neighbors.size)
        val logStr = s"Received ask to initialize Mirror ${vertexId}_${partitionId}"
        ctxLog(logStr)
        replyTo ! VertexEntity.InitializeResponse(s"Initialized Mirror ${vertexId}_${partitionId}")
        Behaviors.same

      // GAS Actions
      case VertexEntity.Begin(stepNum) =>
        ctxLog("Beginning compute")
        value += 1
        Behaviors.same
      case VertexEntity.End =>
        ctxLog("Ordered to stop " + msg)
        // TODO Needed?
        Behaviors.same

      case c: VertexEntity.NeighbourMessage => reactToNeighbourMessage(c)

      case ApplyResult(stepNum, oldVal, newVal) => {
        ctxLog("Received apply value from Main " + newVal)
        localScatter(stepNum, oldVal, newVal, sharding)
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
    if (neighbourCounter(stepNum) == partitionInDegree) {
      val cmd = MirrorTotal(stepNum, summedTotal.get(stepNum))
      val mainRef = sharding.entityRefFor(VertexEntity.TypeKey, main.toString())
      mainRef ! cmd
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
