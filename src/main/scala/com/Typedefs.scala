package com

import akka.actor.typed.ActorRef
import akka.cluster.Member
import com.cluster.graph.{EntityManager, GlobalCoordinator, PartitionCoordinator}
import com.preprocessing.partitioning.oneDim.Mirror
import org.apache.spark.rdd.RDD

/** Store all global type aliases here (typedefs)
  */
object Typedefs {

  type UnweightedEdge = (Int, Int)
  type WeightedEdge = (Int, Int, Int)
  type EitherEdge = Either[RDD[WeightedEdge], RDD[UnweightedEdge]]

  type IndexedUnweightedEdge = (Long, (Int, Int))
  type IndexedWeightedEdge = (Long, (Int, Int, Int))

  type FlaggedWeightedEdge = (((Int, Int, Int), Boolean), Long)
  type FlaggedUnweightedEdge = (((Int, Int), Boolean), Long)
  type EitherFlaggedEdgeRDD = Either[RDD[FlaggedWeightedEdge], RDD[FlaggedUnweightedEdge]]

  type EitherEdgeRDD = Either[RDD[IndexedWeightedEdge], RDD[IndexedUnweightedEdge]]

  // the address of a PartitionCoordinator
  type PCRef = ActorRef[PartitionCoordinator.Command]

  // the address of the GlobalCoordinator
  type GCRef = ActorRef[GlobalCoordinator.Command]
  type EMRef = ActorRef[EntityManager.Command]
  // empty at vertex id i, if no vertex mirror exists in partition
  // otherwise, contains reference to vertex mirror
  // TODO; optimization; a bitSet to indicate existence of mirror in partition
  type MirrorMap = collection.mutable.Map[Int, Mirror]
  type MemberSet = collection.mutable.Set[Member]

  object MirrorMap {
    def empty: MirrorMap = collection.mutable.Map.empty
    def apply(ms: (Int, Mirror)*): MirrorMap = collection.mutable.Map(ms: _*)
  }

  object MemberSet {
    def empty: MemberSet = collection.mutable.Set.empty
    def apply(ms: (Member)*): MemberSet = collection.mutable.Set(ms: _*)
  }

  // an rdd row that represents all the information we need to instantiate a main vertex in akka cluster sharding
  type MainRow = (
      (Int, Int), // (vid, pid)
      Set[_ <: Int], // partitions that contain mirrors
      List[(Int, Int)], // list of outgoing edges on partition pid
      Int // indegree in partition pid
  )

  type TaggedMainRow = (
      (Int, Int), // (vid, pid)
      Set[_ <: Int], // partitions that contain mirrors
      List[(Int, Int, Int)], // list of tagged outgoing edges on partition pid
      Int // indegree in partition pid
  )

  type MirrorRow = (
      (Int, Int), // (vid, pid)
      Int, // partition that contains main
      List[(Int, Int)], // list of outgoing edges on partition pid
      Int // indegree in partition pid
  )

  type TaggedMirrorRow = (
      (Int, Int), // (vid, pid)
      Int, // partition that contains main
      List[(Int, Int, Int)], // list of tagged outgoing edges on partition pid
      Int // indegree in partition pid
  )
}
