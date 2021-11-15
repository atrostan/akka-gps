package com.cluster.graph.entity

import akka.cluster.sharding.typed.scaladsl.EntityTypeKey

final class EntityId(val eType: String, val vertexId: Int, val partitionId: Int) {
  override def toString(): String = s"${eType}_${vertexId}_${partitionId}"

  // A way of resolving the entity TypeKey
  def getTypeKey(): EntityTypeKey[VertexEntity.Command] = {
    VertexEntityType.withName(eType) match {
      case VertexEntityType.Main   => MainEntity.TypeKey
      case VertexEntityType.Mirror => MirrorEntity.TypeKey
    }
  }

}
object EntityId {
  def getTypeFromString(entityId: String): String = {
    entityId.split("_").head
  }
}
