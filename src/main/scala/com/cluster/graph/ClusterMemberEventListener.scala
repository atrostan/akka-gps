package com.cshard

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{Behavior, PostStop}
import akka.cluster.ClusterEvent._
import akka.cluster.typed.{Cluster, Subscribe, Unsubscribe}
import akka.cluster.{Member, MemberStatus}

object ClusterMemberEventListener {

  def apply(nodesUp: collection.mutable.Set[Member]): Behavior[ClusterDomainEvent] = Behaviors.setup[ClusterDomainEvent] { context =>
    Cluster(context.system).subscriptions ! Subscribe(
      context.self,
      classOf[ClusterDomainEvent])

    Behaviors
      .receiveMessage[ClusterDomainEvent] {

        case MemberJoined(member) =>
          //          println("joinyjoin")
          context.log.info(s"$member JOINED")
          Behaviors.same

        case MemberUp(member) =>
          nodesUp += member
          context.log.info(s"$member UP.")
          //          println("current nodes up: ")
          //          nodesUp.foreach{ n  =>
          //            println (n)
          //            println("n.address", n.address)
          //            println("n.hashCode()", n.hashCode())
          //            println("n.roles", n.roles)
          //            println("n.status", n.status)
          //            println("n.uniqueAddress", n.uniqueAddress)
          //          }
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

        //        case ReachabilityChanged(m) =>
        //          context.log.info(s"reachability changed for $m")
        //          Behaviors.same
        //
        //        case SeenChanged(convergence, seenBy) =>
        //          context.log.info(s"convergence is $convergence")
        //          context.log.info(s"$seenBy")
        //          Behaviors.same

        case event =>
          context.log.info(s"not handling ${event.toString}")
          Behaviors.same

      }

      .receiveSignal {
        case (context, PostStop) =>
          Cluster(context.system).subscriptions ! Unsubscribe(
            context.self)
          Behaviors.stopped
      }
  }
}
