package com.chapter7

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings}
import com.chapter7.TemperatureActor.{GetCurrentTemperature, Location, UpdateTemperature}
import akka.pattern.ask
import akka.util.Timeout

import scala.language.postfixOps
import scala.concurrent.duration._
import scala.util.Success
import akka.event.Logging.Error
import com.typesafe.config.ConfigFactory


object ClusterShardingApplication extends App {

  val config = ConfigFactory.load()
//  val configStr = "application-cluster-sharding-1"
  //  val actorSystem = ActorSystem("ClusterSystem", config.getConfig(configStr))
  val actorSystem = ActorSystem("ClusterSystem")


  import actorSystem.dispatcher

  val temperatureActor: ActorRef = ClusterSharding(actorSystem).start(
    typeName = TemperatureActor.shardName,
    entityProps = Props[TemperatureActor],
    settings = ClusterShardingSettings(actorSystem),
    extractEntityId = TemperatureActor.extractEntityId,
    extractShardId = TemperatureActor.extractShardId)

  //Let's simulate some time has passed. Never use Thread.sleep in production!
  Thread.sleep(30000)

  val locations = Vector(
    Location("USA", "Chicago"),
    Location("ESP", "Madrid"),
    Location("FIN", "Helsinki")
  )

  temperatureActor ! UpdateTemperature(locations(0), 1.0)
  temperatureActor ! UpdateTemperature(locations(1), 20.0)
  temperatureActor ! UpdateTemperature(locations(2), -10.0)

  implicit val timeout = Timeout(5 seconds)

  locations.foreach {
    case location =>
      (temperatureActor ? GetCurrentTemperature(location)).onComplete {
        case Success(x: Double) =>
          println(s"Current temperature in $location is $x")
        case _ => println("error")
      }
  }
}