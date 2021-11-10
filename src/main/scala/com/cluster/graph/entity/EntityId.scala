package com.cluster.graph.entity

final class EntityId(eCl: String, vid: Int, pid: Int) {
  val entityClass = eCl
  val vertexId = vid
  val partitionId = pid
  override def toString(): String = s"${entityClass}_${vertexId}_${partitionId}"
}
