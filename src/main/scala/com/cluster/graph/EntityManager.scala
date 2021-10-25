package com.cshard

import akka.actor.Actor
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.ShardRegion.ShardId
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityRef}
import akka.cluster.typed.Cluster
import akka.cluster.typed.ClusterSingleton
import akka.cluster.typed.SingletonActor

import java.io.{File, FileOutputStream}

object EntityManager {

  sealed trait Command

  case class AddOne(counterId: String) extends Command

  case class GetSum(counterId: String) extends Command

  case class WrappedTotal(res: Counter.Response) extends Command

  case class Received(i: Int) extends Command

  object sReceiver {
    final case class sReceive(whom: Int, replyTo: ActorRef[sReceived])
    final case class sReceived(whom: Int, from: ActorRef[sReceive])

    def apply(): Behavior[sReceive] = Behaviors.receive { (context, message) =>
      context.log.info("Hello {}!", message.whom)
      message.replyTo ! sReceived(message.whom, context.self)
      Behaviors.same
    }
  }


  def apply(partitionMap: collection.mutable.Map[Int, Int]): Behavior[Command] = Behaviors.setup { ctx =>
    //    val tracker = new Tracker(role, pid)
    val myShardAllocationStrategy = new MyShardAllocationStrategy(partitionMap)
    val rRef = ctx.spawn(sReceiver(), "sReceiver")
    rRef
    val cluster = Cluster(ctx.system)
    val fos = new FileOutputStream(new File("./tmp"), true)
    Console.withOut(fos) {
      println("leader:", cluster.state.leader)
      println("members ", cluster.state.members)
      println("cluster.selfMember", cluster.selfMember)
      println("cluster.selfMember.roles", cluster.selfMember.roles)
      println("cluster.selfMember.status", cluster.selfMember.status)
      println("cluster.selfMember.address.toString", cluster.selfMember.address.toString)
    }


    val sharding = ClusterSharding(ctx.system)

    val entityType = Entity(Counter.TypeKey) { entityContext =>
      Counter(cluster.selfMember.address.toString, entityContext)
    }
      // the shard allocation decisions are taken by the central ShardCoordinator, which is running as a cluster
      // singleton, i.e. one instance on the oldest member among all cluster nodes or a group of nodes tagged with a
      // specific role.
      .withRole("shard")
      .withStopMessage(Counter.StopCounter)
      .withAllocationStrategy(myShardAllocationStrategy)
    sharding.init(entityType)
    val counterRef: ActorRef[Counter.Response] = ctx.messageAdapter(ref => WrappedTotal(ref))
    Console.withOut(fos) {
      println("counterRef: ", counterRef)
      Behaviors.receiveMessage[Command] {
        case AddOne(cid) =>
          val entityRef: EntityRef[Counter.Command] = sharding.entityRefFor(Counter.TypeKey, cid)
          println("entityRef: ", entityRef)
          entityRef ! Counter.Increment
          Behaviors.same
        case GetSum(cid) =>
          val entityRef: EntityRef[Counter.Command] = sharding.entityRefFor(Counter.TypeKey, cid)
          entityRef ! Counter.GetValue(counterRef)
          Behaviors.same
        case Received(i) =>
          println(s"did it work: ${i}")
          Behaviors.same
        case WrappedTotal(ttl) => ttl match {
          case Counter.SubTtl(eid, subttl) =>
            ctx.log.info("***********************{} total: {} ", eid, subttl)
        }
//          println("Tracker: \n", tracker)
          Behaviors.same
      }
    }
  }
}