package com.cluster.graph.entity

import scala.collection.mutable
import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import akka.actor.typed.{Behavior, ActorRef}
import akka.actor.typed.scaladsl.{Behaviors, AbstractBehavior, ActorContext}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityContext, EntityTypeKey}

import com.cluster.graph.PartitionCoordinator
import VertexEntity._
import com.algorithm.VertexInfo

// Vertex actor
class MainEntity(
    ctx: ActorContext[VertexEntity.Command],
    nodeAddress: String,
    entityContext: EntityContext[VertexEntity.Command]
) extends AbstractBehavior[VertexEntity.Command](ctx)
    with VertexEntity {

  private var mirrors = ArrayBuffer[EntityId]()
  private var pcRef: ActorRef[PartitionCoordinator.Command] = null

  val mirrorCounter: mutable.Map[SuperStep, Int] = new mutable.HashMap().withDefaultValue(0)
  var active: Boolean = vertexProgram.defaultActivationStatus
  var currentValue: VertexValT = vertexProgram.defaultVertexValue
  val okToProceed: mutable.Map[SuperStep, Boolean] = new mutable.HashMap().withDefaultValue(false)

  var value = 0 // Counter TEST ONLY

  // In order for vertices find refs for messages, they need to sharding.entityRefFor by entity id
  val sharding = ClusterSharding(ctx.system)

  override def ctxLog(event: String): Unit = {
//    ctx.log.info(
//    println(
//      s"******************{} ${event} at {}, eid: {}",
//      ctx.self.path,
//      nodeAddress,
//      entityContext.entityId
//    )
  }

  override def onMessage(
      msg: VertexEntity.Command
  ): Behavior[VertexEntity.Command] = {
    msg match {
      case VertexEntity.Initialize(vid, pid, neigh, mrs, inDeg, outDeg, replyTo) =>
        vertexId = vid
        partitionId = pid.toShort
        neighbors ++= neigh
        mirrors ++= mrs
        partitionInDegree = inDeg
        thisVertexInfo = VertexInfo(vertexId, outDeg)

//        val logStr = s"Received ask to initialize Main ${vertexId}_${partitionId}"
//        ctxLog(logStr)
//        ctxLog(neighbors.toString())
//        ctxLog(mirrors.toString())
        replyTo ! InitializeResponse(s"Initialized Main ${vertexId}_${partitionId}")
        Behaviors.same

      // PartitionCoordinator Commands
      case StorePCRef(pc, replyTo) =>
        pcRef = pc
        replyTo ! AckPCLocation()
        Behaviors.same

      // GAS Actions
      case VertexEntity.Begin(0) => {
//        ctxLog("Beginning compute: Step 0")
        applyAndScatter(0, None)
        Behaviors.same
      }

      case VertexEntity.Begin(stepNum) => {
//        ctxLog(s"Beginning compute: Step ${stepNum}")
        okToProceed(stepNum) = true
        applyIfReady(stepNum)
        value += 1
        Behaviors.same
      }

      case VertexEntity.End =>
//        ctxLog("Ordered to stop " + msg)
        // TODO Needed?
        Behaviors.same

      case c: VertexEntity.NeighbourMessage => reactToNeighbourMessage(c)

      case MirrorTotal(stepNum, mirrorTotal) => {
//        ctxLog("Received mirror total " + mirrorTotal)
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

      case GetFinalValue => {
        pcRef ! PartitionCoordinator.FinalValue(vertexId, currentValue)
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
//        ctxLog("get value")
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
//        println(s"step: ${stepNum} term v${this.vertexId}: color:${currentValue}")
        pcRef ! PartitionCoordinator.TerminationVote(stepNum) // TODO change to new PC command

        // Still have to send null messages to neighbours
        localScatter(stepNum, currentValue, None, sharding)
        val cmd = ApplyResult(stepNum, currentValue, None)
        for (mirror <- mirrors) {
          val mirrorRef = sharding.entityRefFor(VertexEntity.TypeKey, mirror.toString())
          mirrorRef ! cmd
        }
      }
      case _ => {
        // Continue

        val newVal = vertexProgram.apply(stepNum, thisVertexInfo, currentValue, total)
        val oldVal = currentValue
        currentValue = newVal
//        println(s"step: ${stepNum} cont v${this.vertexId}: color:${currentValue}")
        val cmd = ApplyResult(stepNum, oldVal, Some(newVal))
        for (mirror <- mirrors) {
//          println(mirror)
          val mirrorRef = sharding.entityRefFor(VertexEntity.TypeKey, mirror.toString())
//          println(mirrorRef)
          mirrorRef ! cmd
        }
        active = !vertexProgram.deactivateSelf(stepNum, oldVal, newVal)
        localScatter(stepNum, oldVal, Some(newVal), sharding)
        pcRef ! PartitionCoordinator.DONE(stepNum) // TODO change to new PC command
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
