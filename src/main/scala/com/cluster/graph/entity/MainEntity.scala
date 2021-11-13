package com.cluster.graph.entity

import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityContext, EntityTypeKey}
import akka.cluster.typed.Cluster
import com.CborSerializable
import com.cluster.graph.{EntityManager, PartitionCoordinator}
import com.cluster.graph.entity.VertexEntity.LocationResponse

// Vertex actor
class MainEntity(
                  ctx: ActorContext[VertexEntity.Command],
                  nodeAddress: String,
                  entityContext: EntityContext[VertexEntity.Command]
                ) extends AbstractBehavior[VertexEntity.Command](ctx)
  with VertexEntity {

  import MainEntity._

  var vertexId = 0
  var partitionId = 0
  private var neighbors: ArrayBuffer[EntityId] = null
  private var mirrors: ArrayBuffer[EntityId] = null
  private var pc: ActorRef[PartitionCoordinator.Command] = null
  // TODO neighbourCounter, summedTotal

  var value = 0 // Counter TEST ONLY

  // In order for vertices to be able to send messages, they need to sharding.entityRefFor by entity id
  val cluster = Cluster(ctx.system)
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
      case Initialize(vid, pid, neigh, mrs, replyTo) =>
        vertexId = vid
        partitionId = pid.toShort
        neighbors = neigh
        mirrors = mrs

        val logStr = s"Received ask to initialize Main ${vertexId}_${partitionId}"
        ctxLog(logStr)
        replyTo ! InitResponse(s"Initialized Main ${vertexId}_${partitionId}")
        Behaviors.same

      // PartitionCoordinator Commands
      case StorePCRef(pcRef, replyTo) =>
        println("partCoord command")
        println(pcRef)
        replyTo ! AckPCLocation()
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
      case MirrorTotal(stepNum, total) =>
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
        ctxLog("Unknown behaviour for main " + msg.toString)
        Behaviors.same
    }
  }
}

object MainEntity {
  val TypeKey: EntityTypeKey[VertexEntity.Command] =
    EntityTypeKey[VertexEntity.Command]("MainEntity")

  // Orchestration
  sealed trait Reply

  case class InitResponse(message: String) extends CborSerializable with Reply

  case class AckPCLocation() extends CborSerializable with Reply

  final case class Initialize(
                               vertexId: Int,
                               partitionId: Int,
                               neighbors: ArrayBuffer[EntityId],
                               mirrors: ArrayBuffer[EntityId],
                               replyTo: ActorRef[InitResponse],
                             ) extends VertexEntity.Command

  final case class StorePCRef(
                               pcRef: ActorRef[PartitionCoordinator.Command],
                               replyTo: ActorRef[AckPCLocation]
                             ) extends VertexEntity.Command


  // GAS
  final case class MirrorTotal(stepNum: Int, total: Int) extends VertexEntity.Command

  def apply(
             nodeAddress: String,
             entityContext: EntityContext[VertexEntity.Command]
           ): Behavior[VertexEntity.Command] = {
    Behaviors.setup(ctx => {
      ctx.setReceiveTimeout(30.seconds, VertexEntity.Idle)
      new MainEntity(ctx, nodeAddress, entityContext)
    })
  }
}
