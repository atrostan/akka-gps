package com.cluster.graph

import akka.actor.typed._
import akka.cluster.Member
import com.typesafe.config.ConfigFactory

object ClusterShardingApp {

  // example of how to define a custom type
  // TODO use more type aliases in project
  //  type PartitionMap = collection.mutable.Map[Int, Int]
  //  object PartitionMap {
  //    def empty: PartitionMap = collection.mutable.Map.empty
  //    def apply(pairs: (Int,Int)*): PartitionMap = collection.mutable.Map(pairs:_*)
  //  }
  //
  //  val myPMap: PartitionMap = collection.mutable.Map[Int,Int]()

  def main(args: Array[String]): Unit = {
    val partitionMap = collection.mutable.Map[Int, Int]()
    if (args.isEmpty) {
      startup("domainListener", 25251, partitionMap)

      val ports = List[Int](25252, 25253, 25254)

      var i: Int = 0;
      ports.foreach(p => {
        partitionMap(i) = p
        startup("shard", p, partitionMap)
        i += 1
      })

      startup("front", 25255, partitionMap)
    } else {
      require(args.size == 2, "Usage: role port")
      startup(args(0), args(1).toInt, partitionMap)
    }
  }

  def startup(role: String, port: Int, partitionMap: collection.mutable.Map[Int, Int]): Unit = {
    // Override the configuration of the port when specified as program argument
    val config = ConfigFactory
      .parseString(
        s"""
      akka.remote.artery.canonical.port=$port
      akka.cluster.roles = [$role]
      """)
      .withFallback(ConfigFactory.load("cluster"))

    var nodesUp = collection.mutable.Set[Member]()

    if (role == "domainListener") {
      // enable ClusterMemberEventListener for logging purposes
       ActorSystem(ClusterMemberEventListener(nodesUp), "ClusterSystem", config)
    }
    else {
      val entityManager = 
        ActorSystem[VertexEntityManager.Command](VertexEntityManager(partitionMap), "ClusterSystem", config)

      if (role == "front") {
        entityManager ! VertexEntityManager.Initialize("5_0")
        entityManager ! VertexEntityManager.Initialize("6_1")
        entityManager ! VertexEntityManager.Initialize("7_2")
        entityManager ! VertexEntityManager.Initialize("8_1")
        
        entityManager ! VertexEntityManager.AddOne("5_0")
        entityManager ! VertexEntityManager.AddOne("6_1")
        entityManager ! VertexEntityManager.AddOne("7_2")
        entityManager ! VertexEntityManager.AddOne("5_0")
        entityManager ! VertexEntityManager.AddOne("6_1")
        entityManager ! VertexEntityManager.AddOne("7_2")
        entityManager ! VertexEntityManager.GetSum("5_0")
        entityManager ! VertexEntityManager.GetSum("6_1")
        entityManager ! VertexEntityManager.GetSum("7_2")
        entityManager ! VertexEntityManager.GetSum("8_1")
      }
    }
  }
}
