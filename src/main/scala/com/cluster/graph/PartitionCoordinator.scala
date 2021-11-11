package com.cluster.graph

import scala.util.Success
import scala.util.Failure
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.typed.Cluster
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.util.Timeout
import com.CborSerializable
import com.cluster.graph.entity.{EntityId, VertexEntity}

import scala.concurrent.duration.DurationInt


/**
 * PartitionCoordinator Actor
 * Exists on each partition of the graph, is aware of all the main vertices
 * on its partition
 * When a main vertex completes its computation for the current superstep,
 * it notifies the PartitionCoordinator
 * When the PartitionCoordinator has received n_i such notifications,
 * where n_i is the number of main vertices on partition i,
 * the PartitionCoordinator notifies the centralCoordinator that partition i
 * has completed the current superstep
 *
 * @param nMains
 * The number of main vertices in the partition assigned to this PartitionCoordinator
 * @param partitionId
 * The partition id of the partition assigned to this PartitionCoordinator
 */

class PartitionCoordinator

object PartitionCoordinator {

  trait Command extends CborSerializable

  trait Response extends CborSerializable

  case class Done(stepNum: Int) extends Command

  case class TerminationVote(stepNum: Int) extends Command

  case class Begin(stepNum: Int) extends Command

  case class BroadcastLocation() extends Command

  private case class AdaptedResponse(message: String) extends Command

  // counts the number of vertices in this partition that have finished their computation for this superstep
  var doneCounter = collection.mutable.Map[Int, Int]().withDefaultValue(0)
  // counts the number of vertices in this partition that have voted to terminate their computation
  var voteCounter = collection.mutable.Map[Int, Int]().withDefaultValue(0)

  def apply(mains: List[EntityId], partitionId: Int): Behavior[Command] = Behaviors.setup {
    val nMains = mains.length

    def locallyDone(stepNum: Int): Boolean = {
      doneCounter(stepNum) + voteCounter(stepNum) == nMains
    }

    def receiveDone(stepNum: Int) = {
      doneCounter(stepNum) += 1
      if (locallyDone(stepNum)) {
        // send localDone(stepNum) to GlobalCoordinator
      }
    }

    def sendToMains() = {
      for (m <- mains) {

      }
    }

    ctx =>
      val pcRef: ActorRef[PartitionCoordinator.Command] = ctx.self
      val cluster = Cluster(ctx.system)
      val sharding = ClusterSharding(ctx.system)


      Behaviors.receiveMessage[Command] {

        case Done(stepNum) =>
          Behaviors.same

        case BroadcastLocation() =>
          implicit val timeout: Timeout = 3.seconds

          println("got disseminate location")
          println("mains")
          for (m <- mains) {
            println(s"sending to: $m")
            val eRef = sharding.entityRefFor(VertexEntity.TypeKey, m.toString)
            ctx.ask(eRef, VertexEntity.NotifyLocation.apply){
              case Success(VertexEntity.LocationResponse(message)) => AdaptedResponse(s"$message")
              case Failure(_) => AdaptedResponse("Location Message failed...")
            }
//            eRef ? VertexEntity.NotifyLocation(ctx.self)
          }
          Behaviors.same
        case AdaptedResponse(message) =>
          ctx.log.info("Got response from hal: {}", message)
          Behaviors.same
      }

  }
}
