package com.cluster.graph

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import sample.cluster.CborSerializable

import scala.concurrent.duration._

//#service
object VertexService {

  sealed trait Command extends CborSerializable
  final case class ProcessText(text: String, replyTo: ActorRef[Response]) extends Command {
    require(text.nonEmpty)
  }
  case object Stop extends Command

  sealed trait Response extends CborSerializable
  final case class JobResult(meanWordLength: Double) extends Response
  final case class JobFailed(reason: String) extends Response

  def apply(workers: ActorRef[VertexWorker.Process]): Behavior[Command] =
    Behaviors.setup { ctx =>
      // if all workers would crash/stop we want to stop as well
      ctx.watch(workers)

      Behaviors.receiveMessage {
        case ProcessText(text, replyTo) =>
          ctx.log.info("Delegating request")
          val words = text.split(' ').toIndexedSeq
          // create per request actor that collects replies from workers
          ctx.spawnAnonymous(StatsAggregator(words, workers, replyTo))
          Behaviors.same
        case Stop =>
          Behaviors.stopped
      }
    }
}

object StatsAggregator {

  sealed trait Event
  private case object Timeout extends Event
  private case class CalculationComplete(length: Int) extends Event

  def apply(words: Seq[String], workers: ActorRef[VertexWorker.Process], replyTo: ActorRef[VertexService.Response]): Behavior[Event] =
    Behaviors.setup { ctx =>
      ctx.setReceiveTimeout(3.seconds, Timeout)
      val responseAdapter = ctx.messageAdapter[VertexWorker.Processed](processed =>
        CalculationComplete(processed.length)
      )

      words.foreach { word =>
        workers ! VertexWorker.Process(word, responseAdapter)
      }
      waiting(replyTo, words.size, Nil)
    }

  private def waiting(replyTo: ActorRef[VertexService.Response], expectedResponses: Int, results: List[Int]): Behavior[Event] =
    Behaviors.receiveMessage {
      case CalculationComplete(length) =>
        val newResults = results :+ length
        if (newResults.size == expectedResponses) {
          val meanWordLength = newResults.sum.toDouble / newResults.size
          replyTo ! VertexService.JobResult(meanWordLength)
          Behaviors.stopped
        } else {
          waiting(replyTo, expectedResponses, newResults)
        }
      case Timeout =>
        replyTo ! VertexService.JobFailed("Service unavailable, try again later")
        Behaviors.stopped
    }

}
//#service
