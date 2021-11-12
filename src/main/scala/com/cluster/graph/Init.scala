package com.cluster.graph

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.util.Timeout
import com.cluster.graph.ClusterShardingApp.{partCoordMap, partitionMap}
import com.cluster.graph.entity.EntityId
import com.preprocessing.partitioning.oneDim.Partitioning
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object Init {
  val waitTime = 30 seconds
  implicit val timeout: Timeout = waitTime

  def initEntityManager(png: Partitioning, config: Config): ActorSystem[EntityManager.Command] = {
    ActorSystem[EntityManager.Command](
      EntityManager(partitionMap, png.mainArray),
      "ClusterSystem", config
    )
  }

  def blockInitPartitionCoordinator(
                                     pc: ActorSystem[PartitionCoordinator.Command],
                                     mains: ArrayBuffer[EntityId],
                                     partitionId: Int
                                   ): Int = {
    implicit val ec = pc.executionContext
    implicit val scheduler = pc.scheduler
    val future: Future[PartitionCoordinator.InitResponse] = pc.ask(ref =>
      PartitionCoordinator.Initialize(
        mains,
        partitionId,
        ref
      )
    )
    val pcInitResult = Await.result(future, waitTime)
    pcInitResult match {
      case PartitionCoordinator.InitResponse(message) =>
        println(message)
        1
      case _ =>
        println(s"Failed to init PartitionCoordinator for partition ${partitionId}")
        0
    }
  }

  def initPartitionCoordinator(
                                partitionId: Int,
                                port: Int,
                                png: Partitioning
                              ): ActorSystem[PartitionCoordinator.Command] = {
    println(s"pc on port: ${port}")
    var nPartitionCoordinatorsInitialized = 0
    // create a partition coordinator
    val partitionCoordinatorConfig = ConfigFactory
      .parseString(
        s"""
      akka.remote.artery.canonical.port=${port}
      akka.cluster.roles = [partitionCoordinator]
      """)
      .withFallback(ConfigFactory.load("cluster"))
    if (partitionId > 0) partCoordMap(port) = partitionId
    val mains = png.mainArray
      .filter(m => m.partition.id == partitionId)
      .map(m => new EntityId("Main", m.id, partitionId))
      .toList
    println(mains)
    val pc = ActorSystem[PartitionCoordinator.Command](
      PartitionCoordinator(mains, partitionId),
      "ClusterSystem", partitionCoordinatorConfig
    )
    nPartitionCoordinatorsInitialized += blockInitPartitionCoordinator(
      pc, collection.mutable.ArrayBuffer(mains: _*), partitionId
    )
    println(nPartitionCoordinatorsInitialized)
    pc
  }

  def getNMainsInitialized(
                            entityManager: ActorSystem[EntityManager.Command],
                          ): Int = {
    // check that all mains have been correctly initialized
    //    entityManager ! EntityManager.GetNMainsInitialized()
    implicit val timeout: Timeout = waitTime
    implicit val ec = entityManager.executionContext
    implicit val scheduler = entityManager.scheduler
    val future: Future[EntityManager.NMainsInitResponse] = entityManager.ask(ref => EntityManager.GetNMainsInitialized(ref))
    val result = Await.result(future, waitTime)
    result match {
      case EntityManager.NMainsInitResponse(totalMainsInitialized) =>
        totalMainsInitialized
      case _ =>
        println("Failed to get number of initialized mains")
        -1
    }
  }

  def getNMirrorsInitialized(
                              entityManager: ActorSystem[EntityManager.Command],
                            ): Int = {
    // check that all mains have been correctly initialized
    //    entityManager ! EntityManager.GetNMainsInitialized()

    implicit val ec = entityManager.executionContext
    implicit val scheduler = entityManager.scheduler
    val future: Future[EntityManager.NMirrorsInitResponse] = entityManager.ask(ref => EntityManager.GetNMirrorsInitialized(ref))
    val result = Await.result(future, waitTime)
    result match {
      case EntityManager.NMirrorsInitResponse(totalMirrorsInitialized) =>
        totalMirrorsInitialized
      case _ =>
        println("Failed to get number of initialized mains")
        -1
    }
  }
}
