package com.cluster.graph.entity

import scala.collection.mutable
import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import akka.actor.typed.{Behavior, ActorRef}
import akka.actor.typed.scaladsl.{Behaviors, AbstractBehavior, ActorContext}
import akka.cluster.sharding.typed.scaladsl.{
  ClusterSharding,
  EntityContext,
  EntityTypeKey
}

import VertexEntity._

// Vertex actor
class MainEntity(
    ctx: ActorContext[VertexEntity.Command],
    nodeAddress: String,
    entityContext: EntityContext[VertexEntity.Command]
) extends AbstractBehavior[VertexEntity.Command](ctx)
    with VertexEntity {

  private var mirrors: ArrayBuffer[EntityId] = null
  private var partitionCoordinator: ActorRef[MainEntity.DummyPCCommand] = null // TODO Change to ParitionCoordinator.Command

  val mirrorCounter: mutable.Map[SuperStep, Int] = new mutable.HashMap()
  var active: Boolean = vertexProgram.defaultActivationStatus
  var currentValue: VertexValT = vertexProgram.defaultVertexValue
  val okToProceed: mutable.Map[SuperStep, Boolean] = new mutable.HashMap()
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
      case Initialize(vid, pid, neigh, mrs, pc) =>
        ctxLog("Initializing Main")
        vertexId = vid
        partitionId = pid.toShort
        neighbors = neigh
        mirrors = mrs
        partitionCoordinator = pc
        Behaviors.same

      // GAS Actions
      case VertexEntity.Begin(0) => {
        ctxLog("Beginning compute: Step 0")
        applyAndScatter(0, None)
        Behaviors.same
      }

      case VertexEntity.Begin(stepNum) => {
        ctxLog(s"Beginning compute: Step ${stepNum}")
        okToProceed(stepNum) = true
        applyIfReady(stepNum)
        value += 1
        Behaviors.same
      }

      case VertexEntity.End =>
        ctxLog("Ordered to stop " + msg)
        // TODO Needed?
        Behaviors.same

      case c: VertexEntity.NeighbourMessage => reactToNeighbourMessage(c)

      case MirrorTotal(stepNum, mirrorTotal) => {
        ctxLog("Received mirror total " + mirrorTotal)
        mirrorTotal match {
          case None => ()
          case Some(mirrorTotal) => {
            val newTotal = summedTotal.get(stepNum) match {
              case None                => mirrorTotal
              case Some(existingTotal) => vertexProgram.sum(existingTotal, mirrorTotal)
            }
            summedTotal.update(stepNum, newTotal)
          }
        }
        mirrorCounter.update(stepNum, mirrorCounter.getOrElse(stepNum, 0) + 1)
        applyIfReady(stepNum)
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
        ctxLog("Unknown behaviour for main " + msg.toString)
        Behaviors.same
    }
  }

  // Perform apply and scatter phases
  def applyAndScatter(stepNum: SuperStep, total: Option[AccumulatorT]): Unit = {
    (active, total) match {
      case (false, None) => {
        // Vote to terminate
        partitionCoordinator ! MainEntity.TerminationVote(stepNum)
      }
      case _ => {
        // Continue
        val newVal = vertexProgram.apply(stepNum, vertexId, currentValue, total)
        val oldVal = currentValue
        currentValue = newVal
        val cmd = ApplyResult(stepNum, oldVal, newVal)
        for (mirror <- mirrors) {
          val mirrorRef = sharding.entityRefFor(mirror.getTypeKey(), mirror.toString())
          mirrorRef ! cmd
        }
        active = !vertexProgram.voteToHalt(oldVal, newVal)
        localScatter(stepNum, oldVal, newVal, sharding)
        partitionCoordinator ! MainEntity.Done(stepNum)
      }
    }

  }

  override def applyIfReady(stepNum: SuperStep): Unit = {
    if (
      okToProceed(stepNum) &&
      mirrorCounter(stepNum) == mirrors.length &&
      neighbourCounter(stepNum) == partitionInDegree
    ) {
      applyAndScatter(stepNum, summedTotal.get(stepNum))
    }
  }
}

object MainEntity {
  val TypeKey: EntityTypeKey[VertexEntity.Command] =
    EntityTypeKey[VertexEntity.Command]("MainEntity")

  // TODO DELETE Placeholder for paritionCoordinator message type
  sealed trait DummyPCCommand
  case class Done(stepNum: Int) extends DummyPCCommand
  case class TerminationVote(stepNum: Int) extends DummyPCCommand

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
