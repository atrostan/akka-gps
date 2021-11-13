package com.cluster.graph.entity

import akka.cluster.sharding.typed.scaladsl.EntityTypeKey

final class EntityId(val eType: String, val vertexId: Int, val partitionId: Int) {
  override def toString(): String = s"${eType}_${vertexId}_${partitionId}"

  // A way of resolving the entity TypeKey
  def getTypeKey(): EntityTypeKey[VertexEntity.Command] = {
    VertexEntityType.withName(eType) match {
      case VertexEntityType.Main => MainEntity.TypeKey
      case VertexEntityType.Mirror => MirrorEntity.TypeKey
      // case _ => 
      //   Logging.Error(this.toString(), Logging.classFor(Logging.ErrorLevel),s"Unknown entity type: ${eType}")
      //   null
    }
  }
  
  // A way of resolving the entity class
  // def getEntityClass(): Class[_ <: VertexEntity] = {
  //   VertexEntityType.withName(eType) match {
  //     case VertexEntityType.Main => classOf[MainEntity]
  //     case VertexEntityType.Mirror => classOf[MirrorEntity]
  //       // case _ => 
  //       //   Logging.Error(this.toString(), Logging.classFor(Logging.ErrorLevel),s"Unknown entity type: ${eType}")
  //       //   null
  //   }
  // }
}
object EntityId {
  def getTypeFromString(entityId: String): String = {
    entityId.split("_").head
  }

  // A way of resolving the entity class based on string entity id
  // def getEntityClassFromString(entityId: String): Class[_ <: VertexEntity] = {
  //   VertexEntityType.withName(getTypeFromString(entityId)) match {
  //     case VertexEntityType.Main => classOf[MainEntity]
  //     case VertexEntityType.Mirror => classOf[MirrorEntity]
  //       // case _ => 
  //       //   Logging.Error(this.toString(), Logging.classFor(Logging.ErrorLevel),s"Unknown entity type: ${eType}")
  //       //   null
  //   }
  // }
}