package com.cluster.graph.entity

import scala.concurrent.duration._
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{Behaviors, AbstractBehavior, ActorContext}
import akka.cluster.sharding.typed.scaladsl.{
  ClusterSharding,
  EntityContext,
  EntityTypeKey,
  EntityRef
}
import com.CborSerializable
import scala.collection.mutable.ArrayBuffer
import com.algorithm.LocalMaximaColouring
import com.algorithm.Colour
import scala.collection.mutable

object VertexEntity {
  // Hard coded for now
  val vertexProgram = LocalMaximaColouring
  type EdgeValT = Int
  type MessageT = Int
  type AccumulatorT = Set[Int]
  type VertexValT = Option[Colour]
  type SuperStep = Int

  sealed trait Command extends CborSerializable
  trait Response extends CborSerializable

  case object StopVertex extends Command
  case object Idle extends Command

  val TypeKey = EntityTypeKey[VertexEntity.Command]("VertexEntity")

  // GAS General Commands
  case class Begin(stepNum: Int) extends VertexEntity.Command
  case object End extends VertexEntity.Command
  final case class NeighbourMessage(stepNum: Int, edgeVal: Option[EdgeValT],  msg: Option[MessageT]) extends VertexEntity.Command

  // Orchestration
  final case class Initialize(
      vertexId: Int,
      partitionId: Int,
      neighbors: ArrayBuffer[EntityId],
      mirrors: ArrayBuffer[EntityId]
  ) extends VertexEntity.Command

  // GAS
  final case class MirrorTotal(stepNum: Int, total: Option[AccumulatorT]) extends VertexEntity.Command

  // Orchestration
  final case class InitializeMirror(
      vertexId: Int,
      partitionId: Int,
      main: EntityId
  ) extends VertexEntity.Command

  // GAS
  final case class ApplyResult(stepNum: Int, oldVal: VertexValT, newVal: VertexValT) extends VertexEntity.Command

  // Counter actions TESTING ONLY
  case object Increment extends VertexEntity.Command
  final case class GetValue(replyTo: ActorRef[VertexEntity.Response]) extends VertexEntity.Command
  case object EchoValue extends VertexEntity.Command
  case class SubTtl(entityId: String, ttl: Int) extends VertexEntity.Response

  val MAIN_ENTITY = MainEntity.getClass().toString()
  val MIRROR_ENTITY = MirrorEntity.getClass().toString()

  def getEntityClass(entityId: String): String = {
    entityId.split("_").head
  }

  def apply(
      nodeAddress: String,
      entityContext: EntityContext[VertexEntity.Command]
  ): Behavior[VertexEntity.Command] = {
    // TODO HACK to enable polymorphism to work together with sharding entityType. Otherwise shard will only use single type
    if (getEntityClass(entityContext.entityId) == MAIN_ENTITY)
      Behaviors.setup(ctx => new MainEntity(ctx, nodeAddress, entityContext))
    else
      Behaviors.setup(ctx => new MirrorEntity(ctx, nodeAddress, entityContext))
  }

}

trait VertexEntity {
  var vertexId: Int = 0
  var partitionId: Short = 0
  var value: Int

  import VertexEntity._

  var partitionInDegree: Int = 0 // TODO need to get this
  var neighbors: ArrayBuffer[EntityId] = ArrayBuffer()

  val summedTotal: mutable.Map[SuperStep, AccumulatorT] = new mutable.HashMap()
  val neighbourCounter: mutable.Map[SuperStep, Int] = new mutable.HashMap()

  def ctxLog(event: String): Unit

  // Check if ready to perform role in the apply phase, then begin if ready
  def applyIfReady(stepNum: SuperStep): Unit

  def localScatter(stepNum: SuperStep, oldValue: VertexValT, newValue: VertexValT): Unit = {
    val msgOption = vertexProgram.scatter(vertexId, oldValue, newValue)
    
    for(neighbor <- neighbors) {
      val cmd = msgOption match {
        case None => NeighbourMessage(stepNum + 1, None, None)
        case Some(msg) => NeighbourMessage(stepNum + 1, Some(0), Some(msg)) // TODO 0 edgeVal for now, we need to implement these
      }
      // TODO send cmd to neighbour
    }
  }

  def reactToNeighbourMessage(neighbourMessage: NeighbourMessage): Behavior[Command] = neighbourMessage match {
    case NeighbourMessage(stepNum, edgeVal, msg) => {
        ctxLog("Received neighbour msg " + msg)
        (edgeVal, msg) match {
          case (None, None) => {
            
          }
          case (Some(edgeVal), Some(msg)) => {
            val gatheredValue = vertexProgram.gather(edgeVal, msg)
            val newSum = summedTotal.get(stepNum) match {
              case Some(total) => vertexProgram.sum(total, gatheredValue) 
              case None => gatheredValue
            }
            summedTotal.update(stepNum, newSum)
            
          }
          case (_, _) => ??? // Shouldn't happen
        }
        neighbourCounter.update(stepNum, neighbourCounter.getOrElse(stepNum, 0) + 1)
        applyIfReady(stepNum)
        Behaviors.same
      }
  }
}
