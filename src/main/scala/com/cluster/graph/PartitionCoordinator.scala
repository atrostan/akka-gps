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
import com.algorithm.VertexProgram

import java.io.{FileWriter, PrintWriter}
import java.util.Calendar

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

  val finalValues = collection.mutable.Map[Int, VertexEntity.VertexValT]()
  val heapMaxSize: Long = Runtime.getRuntime.maxMemory

  def log(s: String) = {
    val pw = new FileWriter("./execLog", true)
    val currTime: String = Calendar.getInstance().getTime().toString
    pw.write(s"[${currTime}]\t${s}\n")
    pw.close()
  }

  def locallyDone(stepNum: Int): Boolean = {
    doneCounter(stepNum) + voteCounter(stepNum) == nMains
  }

  def locallyTerminated(stepNum: Int): Boolean = {
    voteCounter(stepNum) == nMains
  }

  def blockBroadcastLocation(
      mainERef: EntityRef[VertexEntity.Command]
  ): Unit = {
    val future: Future[VertexEntity.AckPCLocation] =
      mainERef.ask(ref => VertexEntity.StorePCRef(pcRef, ref))
    val broadcastResult = Await.result(future, waitTime)
    broadcastResult match {
      case VertexEntity.AckPCLocation() =>
        nMainsAckd += 1
      case _ =>
        println(s"${mainERef} failed to acknowledge ${pcRef}'s location'")
    }
  }
  def exportFinalVals(finalVals: Map[Int, VertexEntity.VertexValT], path: String) = {
    import java.io.PrintWriter
    new PrintWriter(path) {
      finalVals.foreach { case (i, v) =>
        v match {
//          case c: com.algorithm.Colour => write(s"$i, ${c.num}\n")
          case _                       => write(s"$i $v\n")
        }
      }
      close()
    }
  }
  override def onMessage(
      msg: PartitionCoordinator.Command
  ): Behavior[PartitionCoordinator.Command] = {
    msg match {
      case Initialize(mns, pid, replyTo) =>
        mains ++= mns
        partitionId = pid
        nMains = mains.length
        replyTo ! InitializeResponse(
          s"Initialized PC on partition ${pid} with ${mains.length} mains"
        )
        Behaviors.same

      case UpdateGC(gc, replyTo) =>
        gcRef = gc
        val response = s"PC${partitionId} GlobalCoordinator reference updated!"
        replyTo ! UpdateGCResponse(response)
        Behaviors.same

      case DONE(stepNum) =>
        doneCounter(stepNum) += 1
//        log(s"[DC] step ${stepNum} - doneCounter=${doneCounter(stepNum)}; vote counter ${voteCounter(stepNum)}")
        // Get current size of heap in bytes// Get current size of heap in bytes
//        val heapSize: Long = Runtime.getRuntime.totalMemory
        // Get maximum size of heap in bytes. The heap cannot grow beyond this size.// Any attempt will result in an OutOfMemoryException.
        // Get amount of free memory within the heap in bytes. This size will increase // after garbage collection and decrease as new objects are created.
//        val heapFreeSize: Long = Runtime.getRuntime.freeMemory
//        log(s"[[ ${heapSize} + ${heapFreeSize} ] / ${heapMaxSize} ]")
        if (locallyDone(stepNum)) {
          log("locally done")
          log(s"sending DONE(${stepNum}) to ${gcRef}")
          gcRef ! GlobalCoordinator.DONE(stepNum)
        }
        Behaviors.same

      case TerminationVote(stepNum) =>
        voteCounter(stepNum) += 1
//        log(s"[VC] step ${stepNum} - vote counter ${voteCounter(stepNum)}; doneCounter=${doneCounter(stepNum)};")
//        val heapSize: Long = Runtime.getRuntime.totalMemory
//        val heapFreeSize: Long = Runtime.getRuntime.freeMemory

//        log(s"[[ ${heapSize} + ${heapFreeSize} ] / ${heapMaxSize} ]")

        if (locallyTerminated(stepNum)) {
          log("locally terminated")
          gcRef ! GlobalCoordinator.TerminationVote(stepNum)
        } else if (locallyDone(stepNum)) {
          log("locally done-terminated")
          gcRef ! GlobalCoordinator.DONE(stepNum)
        }

        Behaviors.same
      case BEGIN(stepNum) =>
        log(s"got BEGIN(${stepNum}), sending BEGIN to ${mains.size} mains")
        for (m <- mains) {
          val eRef = sharding.entityRefFor(VertexEntity.TypeKey, m.toString)
          eRef ! VertexEntity.Begin(stepNum)
        }
        log("Sent to all mains!")
        Behaviors.same

      case GetNMainsAckd(replyTo) =>
        replyTo ! NMainsAckdResponse(nMainsAckd)
        Behaviors.same

      // Broadcast the partitionCoordinator ActorRef to all the main vertices
      // in this partition
      case BroadcastLocation() =>
        for (m <- mains) {
          val eRef = sharding.entityRefFor(VertexEntity.TypeKey, m.toString)
          blockBroadcastLocation(eRef)
        }
        Behaviors.same

      case AdaptedResponse(message) =>
        ctx.log.info("Got response from hal: {}", message)
        Behaviors.same
        
      case FinalValue(vtx, value) => {
//        println(s"Received value: ${vtx} -> ${value}")
        finalValues += (vtx -> value)
        if(finalValues.size == nMains) {
          log(s"P${pid} got all final values. Exporting to ./result${pid}")
          exportFinalVals(finalValues.toMap, s"result${pid}")
          log("Exported!")
          println(s"sending a map of size ${finalValues.size} to gc")
          gcRef ! GlobalCoordinator.FinalValues(finalValues.toMap)
        }
        Behaviors.same
      }

      case GetFinalValues => {
        for (m <- mains) {
          val eRef = sharding.entityRefFor(VertexEntity.TypeKey, m.toString)
          eRef ! VertexEntity.GetFinalValue
        }
        Behaviors.same
      }
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
  // command/response typedef
  sealed trait Response extends CborSerializable
  sealed trait Command extends CborSerializable

  // Init Sync Commands
  final case class Initialize(
      mains: List[EntityId],
      partitionId: Int,
      replyTo: ActorRef[InitializeResponse]
  ) extends Command
  final case class UpdateGC(gcRef: GCRef, replyTo: ActorRef[UpdateGCResponse]) extends Command
  final case class GetNMainsAckd(replyTo: ActorRef[NMainsAckdResponse]) extends Command

  //  Init Async Commands
  final case class BroadcastLocation() extends Command

  // Init Sync Response
  final case class InitializeResponse(message: String) extends Response
  final case class UpdateGCResponse(message: String) extends Response
  final case class NMainsAckdResponse(n: Int) extends Response

  // GAS
  final case class DONE(stepNum: Int) extends Command
  final case class TerminationVote(stepNum: Int) extends Command
  final case class BEGIN(stepNum: Int) extends Command
  final case object GetFinalValues extends Command
  final case class FinalValue(vtx: Int, value: VertexEntity.VertexValT) extends Command

  private case class AdaptedResponse(message: String) extends Command
  case object Idle extends Command
}
