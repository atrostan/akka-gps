package com.cluster.graph.entity

import scala.concurrent.duration._
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityContext, EntityTypeKey}
import com.CborSerializable

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
      case InitializeMirror(vid, pid, m, replyTo) =>
        vertexId = vid
        partitionId = pid.toShort
        main = m
        val logStr = s"Received ask to initialize Mirror ${vertexId}_${partitionId}"
        ctxLog(logStr)
        replyTo ! InitResponse(s"Initialized Mirror ${vertexId}_${partitionId}")
        Behaviors.same

      // GAS Actions
      case VertexEntity.Begin =>
        ctxLog("Beginning compute")
        value += 1
        Behaviors.same
      case VertexEntity.End =>
        ctxLog("Ordered to stop " + msg)
        // TODO Implement
        Behaviors.same
      case VertexEntity.NeighbourMessage(stepNum, msg) =>
        ctxLog("Received neighbour msg " + msg)
        // TODO Implement
        Behaviors.same
      case ApplyResult(stepNum, total) =>
        ctxLog("Received mirror total " + total)
        // TODO Implement
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
  sealed trait Reply
  case class InitResponse(message: String) extends CborSerializable with Reply

  final case class InitializeMirror(
                                     vertexId: Int,
                                     partitionId: Int,
                                     main: EntityId,
                                     replyTo: ActorRef[InitResponse]
                                   ) extends VertexEntity.Command

  // GAS
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
