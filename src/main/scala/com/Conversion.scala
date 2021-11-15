package com
import akka.actor.typed
import akka.actor.typed.ActorRefResolver
import akka.actor.typed.scaladsl.adapter._
import akka.serialization.Serialization
import scalapb.TypeMapper

import java.nio.charset.StandardCharsets.UTF_8

// argh
// https://github.com/akka/akka/issues/27975
// https://discuss.lightbend.com/t/akka-typed-serialization/4336/10
// https://stackoverflow.com/questions/57427953/in-akka-typed-how-to-deserialize-a-serialized-actorref-without-its-actorsystem
object Conversion {
  type ActorRef[-T] = typed.ActorRef[T] // importable via Conversion._

  lazy val resolver: ActorRefResolver = ActorRefResolver {
    Serialization.getCurrentTransportInformation().system.toTyped
  }

  implicit def mapper[T]: TypeMapper[String, ActorRef[T]] =
    TypeMapper[String, ActorRef[T]](resolver.resolveActorRef)(serialize)

  def deserialize[T](str: String) = resolver.resolveActorRef[T](str)
  def serialize[T](ref: ActorRef[T]) =
    new String(resolver.toSerializationFormat(ref).getBytes(UTF_8), UTF_8)

}
