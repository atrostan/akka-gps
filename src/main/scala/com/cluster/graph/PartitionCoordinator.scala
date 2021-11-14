package com.cluster.graph

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityRef}
import akka.cluster.typed.Cluster
import akka.util.Timeout
import com.CborSerializable
import com.Typedefs.GCRef
import com.cluster.graph.entity.{EntityId, MainEntity, VertexEntity}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

/** PartitionCoordinator Actor Exists on each partition of the graph, is aware of all the main
  * vertices on its partition When a main vertex completes its computation for the current
  * superstep, it notifies the PartitionCoordinator When the PartitionCoordinator has received n_i
  * such notifications, where n_i is the number of main vertices on partition i, the
  * PartitionCoordinator notifies the centralCoordinator that partition i has completed the current
  * superstep
  *
  * @param mns
  *   The main vertices in the partition assigned to this PartitionCoordinator
  * @param pid
  *   The partition id of the partition assigned to this PartitionCoordinator
  */

class PartitionCoordinator(
    ctx: ActorContext[PartitionCoordinator.Command],
    mns: List[EntityId],
    pid: Int
) extends AbstractBehavior[PartitionCoordinator.Command](ctx) {

  import PartitionCoordinator._
  val pcRef: ActorRef[PartitionCoordinator.Command] = ctx.self
  val waitTime = 10 seconds
  // In order for vertices to be able to send messages, they need to sharding.entityRefFor by entity id
  val cluster = Cluster(ctx.system)
  val sharding = ClusterSharding(ctx.system)
  var mains = ArrayBuffer[EntityId]()
  var partitionId = -1
  var nMains = 0
  implicit val timeout: Timeout = waitTime
  implicit val ec = ctx.system.executionContext
  var nMainsAckd = 0
  var gcRef: ActorRef[GlobalCoordinator.Command] = null
  // counts the number of vertices in this partition that have finished their computation for a superstep
  var doneCounter = collection.mutable.Map[Int, Int]().withDefaultValue(0)
  // counts the number of vertices in this partition that have voted to terminate their computation
  var voteCounter = collection.mutable.Map[Int, Int]().withDefaultValue(0)

  def receiveDone(stepNum: Int) = {
    doneCounter(stepNum) += 1
    if (locallyDone(stepNum)) {
      // send localDone(stepNum) to GlobalCoordinator
    }
  }

  def locallyDone(stepNum: Int): Boolean = {
    doneCounter(stepNum) + voteCounter(stepNum) == nMains
  }

  def sendToMains() = {
    for (m <- mains) {}
  }

  override def onMessage(
      msg: PartitionCoordinator.Command
  ): Behavior[PartitionCoordinator.Command] = {
    msg match {
      case Initialize(mns, pid, replyTo) =>
        mains ++= mns
        partitionId = pid
        replyTo ! InitResponse(s"Initialized PC on partition ${pid} with ${mains.length} mains")
        Behaviors.same

      case UpdateGC(gc, replyTo) =>
        gcRef = gc
        val response = s"PC${partitionId} GlobalCoordinator reference updated!"
        replyTo ! UpdateGCResponse(response)
        Behaviors.same

      case Done(stepNum) =>
        Behaviors.same

      // Broadcast the partitionCoordinator ActorRef to all the main vertices
      // in this partition
      case BroadcastLocation() =>
        println("mains in pc, ", mains)
        for (m <- mains) {
          println(s"sending to: $m")
          val eRef = sharding.entityRefFor(VertexEntity.TypeKey, m.toString)
          blockBroadcastLocation(eRef)
          //            eRef ? VertexEntity.NotifyLocation(ctx.self)
        }
        println(s"sent location to ${nMainsAckd} mains")
        Behaviors.same
      case AdaptedResponse(message) =>
        ctx.log.info("Got response from hal: {}", message)
        Behaviors.same
    }
  }

  def blockBroadcastLocation(
      mainERef: EntityRef[VertexEntity.Command]
  ): Unit = {
    val future: Future[MainEntity.AckPCLocation] =
      mainERef.ask(ref => MainEntity.StorePCRef(pcRef, ref))
    val broadcastResult = Await.result(future, waitTime)
    broadcastResult match {
      case MainEntity.AckPCLocation() =>
        nMainsAckd += 1
      case _ =>
        println(s"${mainERef} failed to acknowledge ${pcRef}'s location'")
    }
  }
}

object PartitionCoordinator {

  def apply(
      mains: List[EntityId],
      partitionId: Int
  ): Behavior[PartitionCoordinator.Command] = {

    Behaviors.setup(ctx => {

      val PartitionCoordinatorKey =
        ServiceKey[PartitionCoordinator.Command](s"partitionCoordinator${partitionId}")
      ctx.system.receptionist ! Receptionist.Register(PartitionCoordinatorKey, ctx.self)
      //ctx.setReceiveTimeout(30.seconds, )
      new PartitionCoordinator(ctx, mains, partitionId)
    })
  }

  sealed trait Response extends CborSerializable

  trait Command extends CborSerializable

  final case class Initialize(
      mains: List[EntityId],
      partitionId: Int,
      replyTo: ActorRef[InitResponse]
  ) extends Command

  case class InitResponse(message: String) extends Response

  final case class UpdateGC(gcRef: GCRef, replyTo: ActorRef[UpdateGCResponse]) extends Command

  case class UpdateGCResponse(message: String) extends Response

  case class Done(stepNum: Int) extends Command

  case class TerminationVote(stepNum: Int) extends Command

  case class Begin(stepNum: Int) extends Command

  case class BroadcastLocation() extends Command

  private case class AdaptedResponse(message: String) extends Command

  case object Idle extends Command
}
