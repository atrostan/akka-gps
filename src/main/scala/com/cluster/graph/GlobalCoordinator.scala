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
import com.cluster.graph.entity.VertexEntity
import com.algorithm.VertexProgram
import akka.stream.javadsl.Partition
import com.algorithm.Colour
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

import java.io.{FileWriter, PrintWriter}
import java.util.Calendar

/** The central coordinator monitors the state of every vertex in the graph (via the
  * partitionCoordinator), to ensure all vertices are performing the computation for the same
  * superstep. Vertices continue to the next superstep only when they receive a message from the
  * central coordinator (via the partitionCoordinator) that they may do so
  */

class GlobalCoordinator(ctx: ActorContext[GlobalCoordinator.Command])
    extends AbstractBehavior[GlobalCoordinator.Command](ctx) {

  import GlobalCoordinator._

  val waitTime = 1 minute
  implicit val timeout: Timeout = waitTime
  implicit val ec = ctx.system.executionContext
  implicit val scheduler = ctx.system.scheduler
  val resolver = ActorRefResolver(ctx.system)
  // In order for vertices to be able to send messages, they need to sharding.entityRefFor by entity id
  val cluster = Cluster(ctx.system)
  val sharding = ClusterSharding(ctx.system)
  val pcRefs = collection.mutable.Map[Int, PCRef]()
  // counts the number of partitions that have finished their computation for a superstep
  var doneCounter = collection.mutable.Map[Int, Int]().withDefaultValue(0)
  // counts the number of partitions that have voted to terminate their computation
  var voteCounter = collection.mutable.Map[Int, Int]().withDefaultValue(0)
  var numPartitions = -1
  var numNodes = -1
  var startComputationTime: Long = 0
  var startMS: Long = 0
  var endMS: Long = 0
  val finalValues = collection.mutable.Map[Int, VertexEntity.VertexValT]()
  var nPCFinalValues = 0
  val iterationTimes = collection.mutable.Map[Int, Long]()

  def log(s: String) = {
    val pw = new FileWriter("./globalLog", true)
    val currTime: String = Calendar.getInstance().getTime().toString
    pw.write(s"[${currTime}]\t${s}\n")
    pw.close()
  }

  def globallyDone(stepNum: Int): Boolean = {
    doneCounter(stepNum) + voteCounter(stepNum) == numPartitions
  }

  def globallyTerminated(stepNum: Int): Boolean = {
    voteCounter(stepNum) == numPartitions
  }

  def broadcastBEGINToPCs(stepNum: Int) = {
    for ((pid, pcRef) <- pcRefs) {
      log((pid, pcRef, stepNum).toString())
      pcRef ! PartitionCoordinator.BEGIN(stepNum)
    }
  }

  override def onMessage(msg: GlobalCoordinator.Command): Behavior[GlobalCoordinator.Command] = {
    msg match {

      case Initialize(pcs, ns, replyTo) =>
        pcRefs ++= pcs
        numNodes = ns
        numPartitions = pcRefs.size
        replyTo ! InitializeResponse("Initialized the Global Coordinator")
        Behaviors.same

      case BEGIN() =>
        log("broadcasting begin 0 to pcs")
        startComputationTime = System.nanoTime()
        startMS = System.currentTimeMillis()
        iterationTimes(0) = System.currentTimeMillis()
        broadcastBEGINToPCs(0)
        Behaviors.same

      case DONE(stepNum) =>
        doneCounter(stepNum) += 1
        log(s"gc : step ${stepNum}: done counter${doneCounter(stepNum)}")
        log(s"doneCounter(${stepNum})=${doneCounter(stepNum)}")
        log(s"voteCounter(${stepNum})=${voteCounter(stepNum)}")

        if (globallyDone(stepNum)) {
          log("globally done =========================================================================================================================================")
          log(s"beginning superstep ${stepNum + 1}")
          iterationTimes(stepNum+1) = System.currentTimeMillis()
          broadcastBEGINToPCs(stepNum + 1)
        }
        Behaviors.same

      case TerminationVote(stepNum) =>

        voteCounter(stepNum) += 1
        if (globallyTerminated(stepNum)) {
          val endTime = System.nanoTime
          endMS = System.currentTimeMillis()
          val computationDuration = (endTime - startComputationTime) / 1e9d
          log(s"Internal global coordinator timer - vertex program took: ${computationDuration}")

          val pw = new FileWriter("./times", true)
          pw.write(s"$stepNum $startMS $endMS $computationDuration\n")
          pw.close()

          val iter_pw = new FileWriter("./iterations", true)
          for ((iterNum, startTime) <- iterationTimes) {
            iter_pw.write(s"$iterNum $startTime\n")
          }
          iter_pw.close();
          log("TERMINATION")
          //TODO TERMINATE..?
          nPCFinalValues = 0
          for((pid, pcRef) <- pcRefs) {
            pcRef ! PartitionCoordinator.GetFinalValues
          }
        } else if (globallyDone(stepNum)) {
          iterationTimes(stepNum+1) = System.currentTimeMillis()
          broadcastBEGINToPCs(stepNum + 1)
        }
        Behaviors.same

      case BroadcastRef(gcRef, replyTo) =>
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

      case FinalValues(valueMap) => {
        finalValues ++= valueMap

        nPCFinalValues += 1
        
        // if(nPCFinalValues == numPartitions) {
          // println("Final Values:")

          // for((vtx, value) <- finalValues) {
          //   println(s"${vtx} -> ${value}")
          // }
        // }
        
        Behaviors.same
      }

      case GetFinalValues(replyTo) => {
        if(nPCFinalValues == numPartitions) {
          replyTo ! FinalValuesResponseComplete(finalValues.toMap)
        } else {
          replyTo ! FinalValuesResponseNotFinished
        }
        Behaviors.same
      }
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

  // command/response typedef
  sealed trait Command extends CborSerializable
  sealed trait Response extends CborSerializable

  // GAS Commands
  final case class DONE(stepNum: Int) extends Command
  final case class TerminationVote(stepNum: Int) extends Command
  final case class BEGIN() extends Command
  /**
   * JsonDeserialize tag needed. See open issue: https://github.com/akka/akka/issues/28566
   */
  final case class FinalValues(@JsonDeserialize(keyAs = classOf[Int]) valueMap: collection.immutable.Map[Int, VertexEntity.VertexValT]) extends Command
  final case class GetFinalValues(replyTo: ActorRef[FinalValuesResponse]) extends Command

  // Init Sync Commands
  final case class GetPCRefs(replyTo: ActorRef[GetPCRefsResponse]) extends Command
  final case class BroadcastRef(gcRef: GCRef, replyTo: ActorRef[BroadcastRefResponse])
      extends Command
  final case class Initialize(
      pcs: collection.mutable.Map[Int, PCRef],
      nPartitions: Int,
      replyTo: ActorRef[InitializeResponse]
  ) extends Command

  // Init Sync Responses
  final case class GetPCRefsResponse(pcRefs: collection.mutable.Map[Int, PCRef]) extends Response
  final case class BroadcastRefResponse(message: String) extends Response
  final case class InitializeResponse(message: String) extends Response

  sealed trait FinalValuesResponse extends Response
  final case class FinalValuesResponseComplete(@JsonDeserialize(keyAs = classOf[Int]) valueMap: collection.immutable.Map[Int, VertexEntity.VertexValT]) extends FinalValuesResponse
  final case object FinalValuesResponseNotFinished extends FinalValuesResponse
}
