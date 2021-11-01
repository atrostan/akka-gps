package com.cluster.graph

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityRef}
import akka.cluster.typed.Cluster

// EntityManager actor
// in charge of both:
// (1) front end actions (incrementing counters),
// (2) managing and redirecting actions to respective actors and entities
object EntityManager {

  sealed trait Command

  case class AddOne(counterId: String) extends Command

  case class GetSum(counterId: String) extends Command

  case class WrappedTotal(res: Counter.Response) extends Command

  case class Received(i: Int) extends Command

  def apply(partitionMap: collection.mutable.Map[Int, Int]): Behavior[Command] = Behaviors.setup { ctx =>
    val myShardAllocationStrategy = new MyShardAllocationStrategy(partitionMap)
    val cluster = Cluster(ctx.system)
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

    Behaviors.receiveMessage[Command] {
      case AddOne(cid) =>
        val entityRef: EntityRef[Counter.Command] = sharding.entityRefFor(Counter.TypeKey, cid)
        entityRef ! Counter.Increment
        Behaviors.same

      case GetSum(cid) =>
        val entityRef: EntityRef[Counter.Command] = sharding.entityRefFor(Counter.TypeKey, cid)
        entityRef ! Counter.GetValue(counterRef)
        Behaviors.same

      case Received(i) =>
        Behaviors.same

      case WrappedTotal(ttl) => ttl match {
        case Counter.SubTtl(eid, subttl) =>
          ctx.log.info("***********************{} total: {} ", eid, subttl)
      }
        Behaviors.same
    }
  }
}