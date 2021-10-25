package com.cshard

import scala.concurrent.duration._
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityContext, EntityTypeKey}
import com.CborSerializable

import java.io.{File, FileOutputStream}

//#counter
object Counter {
  sealed trait Command extends CborSerializable

  case object Increment extends Command

  final case class GetValue(replyTo: ActorRef[Response]) extends Command

  case object StopCounter extends Command

  private case object Idle extends Command

  sealed trait Response extends CborSerializable

  case class SubTtl(entityId: String, ttl: Int) extends Response

  val TypeKey = EntityTypeKey[Counter.Command]("Counter")

  def apply(
             nodeAddress: String,
             entityContext: EntityContext[Command],
           ): Behavior[Command] = {
    val fos = new FileOutputStream(new File("./tmp"), true)
    Behaviors.setup { ctx =>
      println("this should be added: ", (nodeAddress, entityContext.entityId, ctx.self.path.toString))

      def updated(value: Int): Behavior[Command] = {
        Behaviors.receiveMessage[Command] {
          case Increment =>
            ctx.log.info("******************{} counting at {},{}, hashcode {}", ctx.self.path, nodeAddress, entityContext.entityId, entityContext.entityId.hashCode)
            updated(value + 1)
          case GetValue(replyTo) =>
            ctx.log.info("******************{} get value at {},{}, hashcode {}", ctx.self.path, nodeAddress, entityContext.entityId, entityContext.entityId.hashCode)
            ctx.log.info("****************** entityId value: ")
            replyTo ! SubTtl(entityContext.entityId, value)
            Behaviors.same
          case Idle =>
            entityContext.shard ! ClusterSharding.Passivate(ctx.self)
            Behaviors.same
          case StopCounter =>
            Behaviors.stopped(() => ctx.log.info("************{} stopping ... passivated for idling.", entityContext.entityId))
        }
      }

      ctx.setReceiveTimeout(30.seconds, Idle)
      updated(0)
    }
  }
}