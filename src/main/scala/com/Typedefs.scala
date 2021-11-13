package com

import akka.actor.typed.ActorRef
import akka.cluster.Member
import com.cluster.graph.{GlobalCoordinator, PartitionCoordinator}
import com.preprocessing.partitioning.oneDim.Mirror

/**
 * Store all global type aliases here (typedefs)
 */
object Typedefs {

  // the address of a PartitionCoordinator
  type PCRef = ActorRef[PartitionCoordinator.Command]

  // the address of the GlobalCoordinator
  type GCRef = ActorRef[GlobalCoordinator.Command]
  // empty at vertex id i, if no vertex mirror exists in partition
  // otherwise, contains reference to vertex mirror
  // TODO; optimization; a bitSet to indicate existence of mirror in partition
  type MirrorMap = collection.mutable.Map[Int, Mirror]
  object MirrorMap {
    def empty: MirrorMap = collection.mutable.Map.empty
    def apply(ms: (Int, Mirror)*): MirrorMap = collection.mutable.Map(ms:_*)
  }

  type MemberSet = collection.mutable.Set[Member]
  object MemberSet {
    def empty: MemberSet = collection.mutable.Set.empty
    def apply(ms: (Member)*): MemberSet = collection.mutable.Set(ms:_*)
  }
}
