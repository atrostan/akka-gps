package com.cluster.graph

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, PostStop}
import akka.cluster.ClusterEvent._
import akka.cluster.MemberStatus
import akka.cluster.typed.{Cluster, Subscribe, Unsubscribe}
import com.CborSerializable
import com.Typedefs.MemberSet

object ClusterMemberEventListener {

  def apply(nodesUp: MemberSet, nNodes: Int): Behavior[ClusterDomainEvent] =
    Behaviors.setup[ClusterDomainEvent] { context =>
      Cluster(context.system).subscriptions ! Subscribe(context.self, classOf[ClusterDomainEvent])

      Behaviors
        .receiveMessage[ClusterDomainEvent] {

          case nMembersUp(replyTo) =>
            replyTo ! nMembersUpResponse(nodesUp.size)
            Behaviors.same

          case MemberJoined(member) =>
            context.log.info(s"$member JOINED")
            Behaviors.same

          case MemberUp(member) =>
            nodesUp += member
            context.log.info(s"$member UP.")
            Behaviors.same

          case MemberExited(member) =>
            context.log.info(s"$member EXITED.")
            Behaviors.same

          case MemberRemoved(m, previousState) =>
            if (previousState == MemberStatus.Exiting) {
              context.log.info(s"Member $m gracefully exited, REMOVED.")
            } else {
              context.log.info(s"$m downed after unreachable, REMOVED.")
            }
            Behaviors.same

          case UnreachableMember(m) =>
            context.log.info(s"$m UNREACHABLE")
            Behaviors.same

          case ReachableMember(m) =>
            context.log.info(s"$m REACHABLE")
            Behaviors.same

          case MemberPreparingForShutdown(m) =>
            context.log.info(s"$m PreparingForShutdown")
            Behaviors.same

          case MemberReadyForShutdown(m) =>
            context.log.info(s"$m ReadyForShutdown")
            Behaviors.same

          case event =>
            context.log.info(s"not handling ${event.toString}")
            Behaviors.same

        }
        .receiveSignal { case (context, PostStop) =>
          Cluster(context.system).subscriptions ! Unsubscribe(context.self)
          Behaviors.stopped
        }
    }

  sealed trait Response extends CborSerializable

  trait Command extends CborSerializable

  case class nMembersUpResponse(n: Int) extends Response

  case class nMembersUp(replyTo: ActorRef[nMembersUpResponse]) extends ClusterDomainEvent
}
