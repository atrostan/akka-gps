package com.cluster.graph

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, ActorRefResolver, Behavior}
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.typed.Cluster
import akka.util.Timeout
import com.CborSerializable
import com.Typedefs.{GCRef, PCRef}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

/** The central coordinator monitors the state of every vertex in the graph (via the
  * partitionCoordinator), to ensure all vertices are performing the computation for the same
  * superstep. Vertices continue to the next superstep only when they receive a message from the
  * central coordinator (via the partitionCoordinator) that they may do so
  */

class GlobalCoordinator(ctx: ActorContext[GlobalCoordinator.Command])
    extends AbstractBehavior[GlobalCoordinator.Command](ctx) {

  import GlobalCoordinator._

  val waitTime = 10 seconds
  implicit val timeout: Timeout = waitTime
  implicit val ec = ctx.system.executionContext
  implicit val scheduler = ctx.system.scheduler
  val resolver = ActorRefResolver(ctx.system)
  // In order for vertices to be able to send messages, they need to sharding.entityRefFor by entity id
  val cluster = Cluster(ctx.system)
  val sharding = ClusterSharding(ctx.system)
  val pcRefs = collection.mutable.Map[Int, PCRef]()
  // counts the number of vertices in this partition that have finished their computation for a superstep
  var doneCounter = collection.mutable.Map[Int, Int]().withDefaultValue(0)
  // counts the number of vertices in this partition that have voted to terminate their computation
  var voteCounter = collection.mutable.Map[Int, Int]().withDefaultValue(0)
  var nPartitions = -1
  var numNodes = -1

  override def onMessage(msg: GlobalCoordinator.Command): Behavior[GlobalCoordinator.Command] = {
    msg match {

      case Initialize(pcs, ns, replyTo) =>
        pcRefs ++= pcs
        numNodes = ns
        nPartitions = pcRefs.size
        replyTo ! InitResponse("Initialized the Global Coordinator")
        Behaviors.same

      case BroadcastRef(gcRef, replyTo) =>
//        val str = Conversion.serialize(gcRef)
//        val deserialized = Conversion.deserialize(str)
//
//        println("str", str)
//        println("deserialized", deserialized)
//        println("deserialized == gcRef", deserialized == gcRef)
        for ((pid, pcRef) <- pcRefs) {
          val f: Future[PartitionCoordinator.UpdateGCResponse] = pcRef.ask(ref => {
            PartitionCoordinator.UpdateGC(gcRef, ref)
          })
          val UpdateGCResponse = Await.result(f, waitTime)
          UpdateGCResponse match {
            case PartitionCoordinator.UpdateGCResponse(message) =>
              println(message)
            case _ =>
              println(s"PC${pid} did not receive GCRef")
              replyTo ! BroadcastRefResponse("GC Broadcast did not complete")
              Behaviors.same
          }
        }
        replyTo ! BroadcastRefResponse("GC Broadcast to all PCs!")
        Behaviors.same

      case GetPCRefs(replyTo) =>
        replyTo ! GetPCRefsResponse(pcRefs)
        Behaviors.same

    }
  }
}

object GlobalCoordinator {

  val GlobalCoordinatorKey = ServiceKey[GlobalCoordinator.Command](s"globalCoordinator")

  def apply(): Behavior[GlobalCoordinator.Command] = {
    Behaviors.setup(ctx => {

      ctx.system.receptionist ! Receptionist.Register(GlobalCoordinatorKey, ctx.self)
      new GlobalCoordinator(ctx)
    })
  }

  sealed trait Response extends CborSerializable

  trait Command extends CborSerializable

  final case class GetPCRefs(replyTo: ActorRef[GetPCRefsResponse]) extends Command

  case class GetPCRefsResponse(pcRefs: collection.mutable.Map[Int, PCRef]) extends Response

  final case class BroadcastRef(gcRef: GCRef, replyTo: ActorRef[BroadcastRefResponse])
      extends Command

  case class BroadcastRefResponse(message: String) extends Response

  final case class Initialize(
      pcs: collection.mutable.Map[Int, PCRef],
      nPartitions: Int,
      replyTo: ActorRef[InitResponse]
  ) extends Command

  case class InitResponse(message: String) extends Response
}
