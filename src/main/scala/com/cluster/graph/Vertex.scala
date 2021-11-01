package com.cluster.graph

import scala.concurrent.duration._
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityContext, EntityTypeKey}
import com.CborSerializable
import scala.collection.mutable.ArrayBuffer
import akka.cluster.sharding.typed.scaladsl.EntityRef


// counter actor
// TODO modify this to encapsulate actions and state for Vertex Actors (both mains, mirrors?)
object Vertex {
  sealed trait Command extends CborSerializable

  final case class Initialize(vertexId: Int, partitionId: Int, neighbors: ArrayBuffer[EntityId], mirrors: ArrayBuffer[EntityId]) extends Command

  final case class InitializeMirror(vertexId: Int, partitionId: Int, main: EntityId) extends Command

  case object Increment extends Command
  
  final case class GetValue(replyTo: ActorRef[Response]) extends Command
  
  case object EchoValue extends Command
  
  case object StopVertex extends Command

  private case object Idle extends Command

  sealed trait Response extends CborSerializable

  case class SubTtl(entityId: String, ttl: Int) extends Response

  val TypeKey: EntityTypeKey[Vertex.Command] = EntityTypeKey[Vertex.Command]("Vertex")

  def apply(
             nodeAddress: String,
             entityContext: EntityContext[Command],
           ): Behavior[Command] = {

    // TODO Convert to class instead of object and make these fields
    // TODO Split into separate main and mirror setup
    var vertexId: Int = 0
    var partitionId: Int = 0
    var neighbors: ArrayBuffer[EntityId] = null // main only
    var mirrors: ArrayBuffer[EntityId] = null // main only
    var main: EntityId = null // mirror only

    Behaviors.setup { ctx =>
      // In order for vertices to be able to send messages, they need to sharding.entityRefFor by entity id
      val sharding = ClusterSharding(ctx.system)

      def updated(value: Int): Behavior[Command] = {
        Behaviors.receiveMessage[Command] {
          case Initialize(vid, pid, neigh, mrs) =>
            ctx.log.info("******************{} initialized vertex at {}, entityId: {}", ctx.self.path, nodeAddress, entityContext.entityId)
            vertexId = vid
            partitionId = pid
            neighbors = neigh
            mirrors = mrs
            Behaviors.same
          case InitializeMirror(vid, pid, m) =>
            ctx.log.info("******************{} initialized mirror vertex at {}, entityId: {}", ctx.self.path, nodeAddress, entityContext.entityId)
            vertexId = vid
            partitionId = pid
            main = m
            Behaviors.same
          case Increment =>
            ctx.log.info("******************{} adding at {},{}", ctx.self.path, nodeAddress, entityContext.entityId)
            updated(value + 1)

          case GetValue(replyTo) =>
            ctx.log.info("******************{} get value at {},{}", ctx.self.path, nodeAddress, entityContext.entityId)
            replyTo ! SubTtl(entityContext.entityId, value)
            Behaviors.same
          case EchoValue =>
            ctx.log.info("******************{} echo value {} at {},{}", ctx.self.path, value, nodeAddress, entityContext.entityId)
            Behaviors.same
          case Idle =>
            entityContext.shard ! ClusterSharding.Passivate(ctx.self)
            Behaviors.same

          case StopVertex =>
            Behaviors.stopped(() => ctx.log.info("************{} stopping ... passivated for idling.", entityContext.entityId))
        }
      }

      ctx.setReceiveTimeout(30.seconds, Idle)
      updated(0)
    }
  }
}
