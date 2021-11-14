package com.cluster.graph

import akka.cluster.sharding.typed.{ShardingEnvelope, ShardingMessageExtractor}

// TODO Review if this makes sense. Hard to find good examples. See ShardRegion and ShardingMessageExtractor classes.
// private final class VertexIdExtractor(shards: Int) extends HashCodeMessageExtractor(shards) {
//   override def entityId(message: VertexEntity.Command): String = s"${message.vertexId}_${message.partitionId}" // TODO Would be ideal to have this
//   override final def shardId(entityId: String): String = VertexIdExtractor.shardId(entityId, shards)
// }

// NOTE: With Envelope, may be better off without
final class VertexIdExtractor[M](val numberOfShards: Int)
    extends ShardingMessageExtractor[ShardingEnvelope[M], M] {

  override def entityId(envelope: ShardingEnvelope[M]): String =
    envelope.entityId // TODO Figure out how to do // VertexIdExtractor.shardId(envelope)

  override def shardId(entityId: String): String =
    VertexIdExtractor.shardId(entityId, numberOfShards)

  override def unwrapMessage(envelope: ShardingEnvelope[M]): M =
    envelope.message
}

object VertexIdExtractor {
  // private def entityId(envelope: ShardingEnvelope[M]): String = s"${envelope.vertexId}_${envelope.partitionId}"
  private def shardId(entityId: String, numberOfShards: Int): String =
    (entityId.split("_").last.toInt % numberOfShards).toString
}

// TODO Use this to potentially be able to override entityId with custom structure above
/** Re-implementation of envelope type that may be used with Cluster Sharding.
  *
  * Cluster Sharding provides a default [[HashCodeMessageExtractor]] that is able to handle these
  * types of messages, by hashing the entityId into into the shardId. It is not the only, but a
  * convenient way to send envelope-wrapped messages via cluster sharding.
  *
  * The alternative way of routing messages through sharding is to not use envelopes, and have the
  * message types themselves carry identifiers.
  *
  * @param entityId
  *   The business domain identifier of the entity.
  * @param message
  *   The message to be send to the entity.
  * @throws `InvalidMessageException`
  *   if message is null.
  */
// final case class VertexShardingEnvelope[M](entityId: String, message: M) extends WrappedMessage {
//   if (message == null) throw InvalidMessageException("[null] is not an allowed message")
// }
