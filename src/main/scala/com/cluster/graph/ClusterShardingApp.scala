package com.cshard

import akka.actor.typed._
import akka.cluster.Member
import com.typesafe.config.ConfigFactory

object ClusterShardingApp {

  type PartitionMap = collection.mutable.Map[Int, Int]
  object PartitionMap {
    def empty: PartitionMap = collection.mutable.Map.empty
    def apply(pairs: (Int,Int)*): PartitionMap = collection.mutable.Map(pairs:_*)
  }

  val myPMap: PartitionMap = collection.mutable.Map[Int,Int]()

  type ImPartitionMap = collection.immutable.Map[Int, Int]
  type MyType = (String, Int, Double)

  val myType: MyType = ("s", 3, 3.2)


  def main(args: Array[String]): Unit = {
    var pid: Int = 0;
    println(args)
    val partitionMap = collection.mutable.Map[Int, Int]()
    if (args.isEmpty) {
      startup("domainListener", 25251, partitionMap)

      val ports = List[Int](25252, 25253, 25254)

      var i: Int = 0;
      ports.foreach(p => {
        partitionMap(i) = p
        startup("shard", p, partitionMap)
        i+=1
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
      .withFallback(ConfigFactory.load("clustereg"))

    //    var nodesUp = collection.mutable.Set[String]()
    var nodesUp = collection.mutable.Set[Member]()


    if (role == "domainListener") {
      ActorSystem(ClusterMemberEventListener(nodesUp), "ClusterSystem", config)
    } else if (role == "tracker") {

    }
    else {

      val entityManager = ActorSystem[EntityManager.Command](
        EntityManager(partitionMap), "ClusterSystem", config)
      //    entityManager.systemActorOf(cluster)

      //      entityManager.
      //    Await.result(entityManager, 15)
      //      entityManager.
      if (role == "front") {
        entityManager ! EntityManager.AddOne("9013")
        entityManager ! EntityManager.AddOne("9014")
        entityManager ! EntityManager.AddOne("9013")
        entityManager ! EntityManager.AddOne("9015")
        entityManager ! EntityManager.AddOne("9013")
        entityManager ! EntityManager.AddOne("9014")
        entityManager ! EntityManager.AddOne("9014")
        entityManager ! EntityManager.AddOne("9013")
        entityManager ! EntityManager.AddOne("9015")
        entityManager ! EntityManager.AddOne("9015")
        entityManager ! EntityManager.AddOne("9016")
        entityManager ! EntityManager.GetSum("9014")
        entityManager ! EntityManager.GetSum("9015")
        entityManager ! EntityManager.GetSum("9013")
        entityManager ! EntityManager.GetSum("9016")
//        entityManager ! EntityManager.GetSum("9216")
//        entityManager ! EntityManager.GetSum("9116")
//        entityManager ! EntityManager.GetSum("90136")
//        entityManager ! EntityManager.GetSum("90126")
//        entityManager ! EntityManager.GetSum("90136")
//        entityManager ! EntityManager.GetSum("90126")
//        entityManager ! EntityManager.GetSum("9116")
      }

    }

  }

}