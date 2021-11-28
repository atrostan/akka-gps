package com.cluster.graph.entity

import scala.concurrent.duration._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityContext, EntityTypeKey}
import com.CborSerializable
import com.algorithm._
import com.cluster.graph.PartitionCoordinator
import com.algorithm.VertexInfo

object VertexEntity {
  // Hard coded for now
  val vertexProgram = LocalMaximaColouring
  type EdgeValT = Int
  type MessageT = Int
  type AccumulatorT = Set[Int]
  type VertexValT = Option[Colour]
  type SuperStep = Int

  // Commands

  sealed trait Command extends CborSerializable
  trait Response extends CborSerializable

  case object StopVertex extends Command
  case object Idle extends Command

  val TypeKey = EntityTypeKey[VertexEntity.Command]("VertexEntity")

  // GAS Commands
  case class Begin(stepNum: Int) extends Command
  case object End extends Command
  final case class NeighbourMessage(stepNum: Int, edgeVal: Option[EdgeValT], msg: Option[MessageT])
      extends Command
  final case class MirrorTotal(stepNum: Int, total: Option[AccumulatorT]) extends Command
  final case class ApplyResult(stepNum: Int, oldVal: VertexValT, newVal: Option[VertexValT]) extends Command
  final case object GetFinalValue extends Command

  // PartitionCoordinator Commands
  final case class NotifyLocation(replyTo: ActorRef[LocationResponse]) extends Command
  final case class LocationResponse(message: String) extends Response
  final case class StorePCRef(
      pcRef: ActorRef[PartitionCoordinator.Command],
      replyTo: ActorRef[AckPCLocation]
  ) extends Command
  final case class AckPCLocation() extends Response

  // Orchestration
  final case class Initialize(
      vertexId: Int,
      partitionId: Int,
      neighbors: ArrayBuffer[EntityId],
      mirrors: ArrayBuffer[EntityId],
      inDegree: Int,
      replyTo: ActorRef[InitializeResponse]
  ) extends Command
  final case class InitializeMirror(
      vertexId: Int,
      partitionId: Int,
      main: EntityId,
      neighs: ArrayBuffer[EntityId],
      inDegree: Int,
      replyTo: ActorRef[InitializeResponse]
  ) extends Command

  // Init Sync Response
  final case class InitializeResponse(message: String) extends Response


  // Counter actions TESTING ONLY
  case object Increment extends Command
  final case class GetValue(replyTo: ActorRef[VertexEntity.Response]) extends Command
  case object EchoValue extends Command
  case class SubTtl(entityId: String, ttl: Int) extends VertexEntity.Response

  def apply(
      nodeAddress: String,
      entityContext: EntityContext[VertexEntity.Command]
  ): Behavior[VertexEntity.Command] = {
    // TODO HACK to enable polymorphism to work together with sharding entityType. Otherwise shard will only use single type
    VertexEntityType.withName(EntityId.getTypeFromString(entityContext.entityId)) match {
      case VertexEntityType.Main =>
        Behaviors.setup(ctx => new MainEntity(ctx, nodeAddress, entityContext))
      case VertexEntityType.Mirror =>
        Behaviors.setup(ctx => new MirrorEntity(ctx, nodeAddress, entityContext))
    }
  }

}

trait VertexEntity {
  import VertexEntity._

  // Vertex Characteristics/Topology
  var vertexId: Int = 0
  var partitionId: Short = 0
  var partitionInDegree: Int = 0 // TODO need to get this
  var neighbors: ArrayBuffer[EntityId] = ArrayBuffer()
  // TODO edgeVal perhaps part of neighbours list (tuple of neigh,edgeVal). And have default value, from Vertex program?

  // Dynamic Computation Values
  val summedTotal: mutable.Map[SuperStep, AccumulatorT] = new mutable.HashMap()
  val neighbourCounter: mutable.Map[SuperStep, Int] = new mutable.HashMap().withDefaultValue(0)
  var value: Int

  var thisVertexInfo: VertexInfo = null

  def ctxLog(event: String): Unit

  // Check if ready to perform role in the apply phase, then begin if ready
  def applyIfReady(stepNum: SuperStep): Unit

  def localScatter(
      stepNum: SuperStep,
      oldValue: VertexValT,
      newValue: Option[VertexValT],
      shardingRef: ClusterSharding
  ): Unit = {
    val msgOption: Option[MessageT] = newValue.flatMap(vertexProgram.scatter(stepNum, thisVertexInfo, oldValue, _))

    for (neighbor <- neighbors) {
      // TODO 0 edgeVal for now, we need to implement these. Depends on neighbor!
      val cmd = msgOption match {
        case None      => NeighbourMessage(stepNum + 1, None, None)
        case Some(msg) => NeighbourMessage(stepNum + 1, Some(0), Some(msg))
      }
      val neighbourRef = shardingRef.entityRefFor(VertexEntity.TypeKey, neighbor.toString())
      neighbourRef ! cmd
    }
  }

  def reactToNeighbourMessage(neighbourMessage: NeighbourMessage): Behavior[Command] =
    neighbourMessage match {
      case NeighbourMessage(stepNum, edgeVal, msg) => {
        ctxLog("Received neighbour msg " + msg)
        (edgeVal, msg) match {
          case (None, None) => {
            // nothing
          }
          case (Some(edgeVal), Some(msg)) => {
            val gatheredValue = vertexProgram.gather(edgeVal, msg)
            val newSum = summedTotal.get(stepNum) match {
              case Some(total) => vertexProgram.sum(total, gatheredValue)
              case None        => gatheredValue
            }
            summedTotal.update(stepNum, newSum)

          }
          case (_, _) => ??? // Shouldn't happen
        }
        neighbourCounter.update(stepNum, neighbourCounter.getOrElse(stepNum, 0) + 1)
//        println(this.vertexId, neighbourCounter(stepNum), this.partitionId)
        applyIfReady(stepNum)
        Behaviors.same
      }
    }
}

// Types of VertexEntities available in shard // TODO Part of HACK, extra coupling
object VertexEntityType extends Enumeration {
  type VertexEntityType = Value
  val Main = Value("Main")
  val Mirror = Value("Mirror")
}
