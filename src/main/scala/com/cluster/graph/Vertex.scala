package com.cluster.graph

import scala.concurrent.duration._
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.internal.EntityTypeKeyImpl
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityContext, EntityRef, EntityTypeKey}
import com.CborSerializable
import com.cluster.graph.VertexEntityManager.WrappedTotal

import scala.collection.mutable.ArrayBuffer

// final class IdString extends String {
//   override def hashCode(): Int = {
//     var accum: String
//     for (c: Char <- this.value) {
//       if (c != '.') accum += c // else should break but doesn't
//     }
//     accum.toInt
//   }
// }

// /**
//  * Parameter to `createBehavior` function in [[Entity.apply]].
//  *
//  * Cluster Sharding is often used together with [[akka.persistence.typed.scaladsl.EventSourcedBehavior]]
//  * for the entities. See more considerations in [[akka.persistence.typed.PersistenceId]].
//  * The `PersistenceId` of the `EventSourcedBehavior` can typically be constructed with:
//  * {{{
//  * PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId)
//  * }}}
//  *
//  * @param entityTypeKey the key of the entity type
//  * @param entityId the business domain identifier of the entity
//  */
// final class EntityContextOverride[M](
//     val entityTypeKey: EntityTypeKey[M],
//     val entityId: IdString,
//     val shard: ActorRef[ClusterSharding.ShardCommand]) extends EntityContext {
//       /**
//        * INTERNAL API
//        */
//       @InternalApi
//       private[akka] def toJava: akka.cluster.sharding.typed.javadsl.EntityContext[M] =
//         new akka.cluster.sharding.typed.javadsl.EntityContext[M](
//           entityTypeKey.asInstanceOf[EntityTypeKeyImpl[M]],
//           entityId,
//           shard)
//     }


// counter actor
// TODO modify this to encapsulate actions and state for Vertex Actors (both mains, mirrors?)
object Vertex {
  sealed trait Command extends CborSerializable

  final case class Initialize(mrrs: List[String]) extends Command

  case object Increment extends Command

  case object Initialize extends Command

  final case class GetValue(replyTo: ActorRef[Response]) extends Command

  case object StopVertex extends Command

  private case object Idle extends Command

  sealed trait Response extends CborSerializable

  case class SubTtl(entityId: String, ttl: Int) extends Response

  val TypeKey = EntityTypeKey[Vertex.Command]("Vertex")

  var i = 0;

  var mirrors = ArrayBuffer[String]()

  def apply(
             nodeAddress: String,
             entityContext: EntityContext[Command],
           ): Behavior[Command] = {

    Behaviors.setup { ctx =>
      val sharding = ClusterSharding(ctx.system)

      i = nodeAddress.length

      def updated(value: Int): Behavior[Command] = {
        Behaviors.receiveMessage[Command] {
          case Initialize(mrrs: List[String]) =>
            ctx.log.info("******************{} init at {},{}", ctx.self.path, nodeAddress, entityContext.entityId)

            mrrs match {
              case ms => mrrs.foreach(mrr => { mirrors += mrr} )
              case Nil => mirrors
            }

            Behaviors.same
          case Increment =>
            ctx.log.info("******************{} counting at {},{}", ctx.self.path, nodeAddress, entityContext.entityId)
            println("called increment, arre my mirrors updated?")
            mirrors.foreach(mirror => {
              println(mirror)
              val entityRef: EntityRef[Vertex.Command] = sharding.entityRefFor(Vertex.TypeKey, mirror)
              val counterRef: ActorRef[Vertex.Response] = ctx.messageAdapter(ref => WrappedTotal(ref))

              entityRef ! Vertex.GetValue(counterRef)
            })
            updated(value + 1)
          case GetValue(replyTo) =>
            ctx.log.info("******************{} get value at {},{}", ctx.self.path, nodeAddress, entityContext.entityId)
            replyTo ! SubTtl(entityContext.entityId, value)
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
