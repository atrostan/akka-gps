package com.cluster.graph

import akka.actor.ActorRef
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion.ShardId

import scala.collection.immutable
import scala.concurrent.Future

//https://github.com/haghard/safe-chat/blob/1a961ccb228c3a2ceb63b86ff79a327256040201/src/main/scala/com/safechat/actors/DynamicLeastShardAllocationStrategy.scala
class MyShardAllocationStrategy(
    partitionMap: collection.mutable.Map[Int, Int]
) extends ShardAllocationStrategy
    with Serializable {

  private[this] final val emptyRebalanceResult = Future.successful(Set.empty[ShardId])

  // a map between partition ids to their corresponding shard actor refs
  private val shardMap = collection.mutable.Map[Int, ActorRef]()

  // TODO there might be a need to include `another level of indirection` to map between shardId <-> partitionId
  // private val partitionShardMap = collection.mutable.Map[ShardId, Int]()

  override def allocateShard(
      requester: ActorRef,
      shardId: ShardId,
      currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]
  ): Future[ActorRef] = {
    // use partitionMap to assign shards to nodes (ActorRefs)
    if (shardMap.isEmpty) {
      // first call to allocate shard; create a set of the Shard ActorRefs
      val shardActorRefs = collection.mutable.Set[ActorRef]()
      val partitionIds = collection.mutable.Set[Int]()

      for ((actorRef, _) <- currentShardAllocations) {
        shardActorRefs.add(actorRef)
      }
      for ((partitionId, _) <- partitionMap) {
        partitionIds.add(partitionId)
      }

      // iterate through the partitionMap to map each nodeId to the Shard ActorRef that corresponds to that node
      for ((partitionId, nodeId) <- partitionMap) {
        val actorRefMatch: collection.mutable.Set[ActorRef] =
          shardActorRefs.filter(r => r.path.toString.contains(nodeId.toString))
        if (!actorRefMatch.isEmpty) { // nodeId corresponds to an actor ref path that contains that nodeId
          shardMap(partitionId) = actorRefMatch.head // add to the shardMap
          shardActorRefs.remove(actorRefMatch.head)
          partitionIds.remove(partitionId)
        }
      }

      // the remaining actor ref corresponds to the ShardCoordinator, so add the last nodeId to the Shard Map
      // TODO ensure exactly 1 element in both sets (hacky)
      shardMap(partitionIds.head) = shardActorRefs.head
    }

    // currentShardAllocations :+ shardId // append shard to the requester
    // val (regionWithLeastShards, _) = currentShardAllocations.minBy { case (_, v) ⇒ v.size }

    val mappedActorRef: ActorRef = shardMap(shardId.toInt)
    //      Future.successful(regionWithLeastShards)
    Future.successful(mappedActorRef)

  }

  override def rebalance(
      currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
      rebalanceInProgress: Set[ShardId]
  ): Future[Set[ShardId]] = {
    emptyRebalanceResult // no need to rebalance; static partitioning
  }
}
