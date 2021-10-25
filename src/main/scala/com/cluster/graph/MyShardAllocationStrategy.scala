package com.cshard

import akka.actor.ActorRef
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion.ShardId

import java.io.{File, FileOutputStream}
import scala.collection.immutable
import scala.concurrent.Future

//https://github.com/haghard/safe-chat/blob/1a961ccb228c3a2ceb63b86ff79a327256040201/src/main/scala/com/safechat/actors/DynamicLeastShardAllocationStrategy.scala
//type PartitionMap =
class MyShardAllocationStrategy(
                                 partitionMap: collection.mutable.Map[Int, Int]
                               ) extends ShardAllocationStrategy
  with Serializable {

  private[this] final val emptyRebalanceResult = Future.successful(Set.empty[ShardId])

  // a map between partition ids to their corresponding shard actor refs
  private val shardMap = collection.mutable.Map[Int, ActorRef]()

  //  def this(nPartitions: Int, partitionMap: collection.mutable.Map[String, Int]) =
  //    this(nPartitions, partitionMap)

  override def allocateShard(
                              requester: ActorRef,
                              shardId: ShardId,
                              currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]
                            ): Future[ActorRef] = {
    // use partitionMap to assign shards to nodes (ActorRefs)
    val fos = new FileOutputStream(new File("./tmp"), true)
    Console.withOut(fos) {

      if (shardMap.isEmpty) {
        // first call to allocate shard
        println("first call to allocate shard")
        // create a set of the Shard ActorRefs
        val shardActorRefs = collection.mutable.Set[ActorRef]()
        val partitionIds = collection.mutable.Set[Int]()

        for ((actorRef, _) <- currentShardAllocations) {
          shardActorRefs.add(actorRef)
        }
        for ((partitionId, _) <- partitionMap) {
          partitionIds.add(partitionId)
        }
        println(s"shardActorRefs: ${shardActorRefs}")
        println(s"partitionIds: ${partitionIds}")
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
        shardMap.foreach(println)
      } else {
        println("recalling allocate shard")
        shardMap.foreach(println)
      }


      println("-" * 68)
      println("called allocateShard")
      println("-" * 68)
      println("requester: ", requester)
      println("requester.path.address", requester.path.address)
      println("pmap in allocateShard:", partitionMap)
      println(s"shardId {}, {}", shardId, shardId.toInt)
      println("currentShardAllocations:\n")


      for ((actorRef, allocatedShards) <- currentShardAllocations) {
        println(actorRef)
        println(actorRef.path)
        if (allocatedShards.nonEmpty) {
          for (sid <- allocatedShards) {
            println("sid", sid)
          }
        }
      }


      //    currentShardAllocations :+ shardId // append shard to the requester
      val (regionWithLeastShards, _) = currentShardAllocations.minBy { case (_, v) ⇒ v.size }

      val mappedActorRef: ActorRef = shardMap(shardId.toInt)
      println(s"mappedActorRef: ${mappedActorRef}")
      println("adding to: ", mappedActorRef)
//      Future.successful(regionWithLeastShards)
      Future.successful(mappedActorRef)
    }

  }

  override def rebalance(
                          currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
                          rebalanceInProgress: Set[ShardId]
                        ): Future[Set[ShardId]] = {
    val fos = new FileOutputStream(new File("./tmp"), true)

    Console.withOut(fos) {

      println("-" * 68)
      println("called rebalanceInProgress")
      println("-" * 68)
      println("currentShardAllocations.map(println)")
      println(currentShardAllocations.map(println))
      println("rebalanceInProgress.map(println)")
      println(rebalanceInProgress.map(println))

      println("-" * 68)
      println("Shoulda done some work")
      println("-" * 68)
      for ((actorRef, allocatedShards) <- currentShardAllocations) {
        println(actorRef)
        if (allocatedShards.nonEmpty) {
          //            println(allocatedShards)
          for (sid <- allocatedShards) {
            println("sid", sid)
          }
        }
      }
    }
    emptyRebalanceResult // no need to rebalance; static partitioning
  }
}