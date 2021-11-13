package com

import akka.actor.typed.{ActorRef, ActorRefResolver}

import java.nio.charset.StandardCharsets

object Util {
  def serializeActorRef(resolver: ActorRefResolver, ref: ActorRef[_]): String = {
    val serializedActorRef: Array[Byte]= resolver.toSerializationFormat(ref).getBytes(StandardCharsets.UTF_8)
    new String(serializedActorRef, StandardCharsets.UTF_8)
  }
//  def deserializeActorRef(resolver: ActorRefResolver, str: String, T: type): ActorRef[Any] = {
//    resolver.resolveActorRef[SomeType.type](str)
//  }
}
