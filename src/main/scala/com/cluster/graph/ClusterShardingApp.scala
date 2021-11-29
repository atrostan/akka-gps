package com.cluster.graph

import akka.actor.typed._
import akka.cluster.{ClusterEvent, Member}
import com.Typedefs.{EMRef, GCRef, PCRef}
import com.cluster.graph.EntityManager.{InitializeMains, InitializeMirrors}
import com.cluster.graph.Init._
import com.cluster.graph.PartitionCoordinator.BroadcastLocation
import com.cluster.graph.entity.{EntityId, VertexEntityType}
import com.preprocessing.partitioning.Util.{readMainPartitionDF, readMirrorPartitionDF, readWorkerPathsFromYaml}
import com.preprocessing.partitioning.oneDim.{Main, Mirror}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import akka.actor.typed.scaladsl.AskPattern.Askable
import scala.collection.mutable.ArrayBuffer
import com.cluster.graph.entity.{EntityId, VertexEntityType}
import com.preprocessing.partitioning.oneDim.{Main, Mirror}
import com.algorithm.Colour
import scala.concurrent.{Await, Future}
import akka.util.Timeout
import scala.concurrent.duration._
import com.cluster.graph.GlobalCoordinator.FinalValuesResponseComplete
import com.cluster.graph.GlobalCoordinator.FinalValuesResponseNotFinished

object ClusterShardingApp {

  val partitionMap = collection.mutable.Map[Int, Int]()
  val partCoordMap = collection.mutable.Map[Int, Int]()
  val numberOfShards = ConfigFactory
    .load("cluster")
    .getInt("akka.cluster.sharding.number-of-shards")

  // number of nodes in the cluster =
  // (partitionCoordinator/shard) * nPartitions + domainlistener + front/globalCoordinator
  val nNodes = numberOfShards + 2

  def createConfig(role: String, port: Int): Config = {
    val config = ConfigFactory
      .parseString(s"""
      akka.remote.artery.canonical.port = $port
      akka.cluster.roles = [$role]
      akka.cluster.seed-nodes = [
        "akka://ClusterSystem@127.0.0.1:25251"
      ]
      akka.cluster.role.shard.min-nr-of-members = $numberOfShards

      """)
      .withFallback(ConfigFactory.load("cluster"))
    config
  }

  def main(args: Array[String]): Unit = {

    val appName: String = "akka.clusterShardingApp"
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark: SparkSession = SparkSession.builder.getOrCreate

    val hadoopConfig = sc.hadoopConfiguration
    hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    val workerPaths = "src/main/resources/paths.yaml"
    // a map between partition ids to location on hdfs of mains, mirrors for that partition
    val workerMap: Map[Int, String] = readWorkerPathsFromYaml(workerPaths: String)

    val png = initGraphPartitioning(numberOfShards)

    val config = ConfigFactory.load("cluster")
    println(config.getConfig("ec2"))
    val nodesUp = collection.mutable.Set[Member]()

    println(s"Initializing cluster with ${nNodes} compute nodes")

    println("Initializing domain listener")
    val domainListenerPort = 25251
    val domainListenerRole = "domainListener"
    val domainListenerConfig = createConfig(domainListenerRole, domainListenerPort)
    val domainListener: ActorSystem[ClusterEvent.ClusterDomainEvent] = ActorSystem(
      ClusterMemberEventListener(nodesUp, nNodes),
      "ClusterSystem",
      domainListenerConfig
    )

    val shardPorts = ArrayBuffer[Int](25252, 25253, 25254, 25255)
    val shardActors = ArrayBuffer[ActorSystem[EntityManager.Command]]()
    var pid = 0
    var nMains = 0
    var nMirrors = 0
    for (shardPort <- shardPorts) {
      val path = workerMap(pid)
      val mains = readMainPartitionDF(path + "/mains", spark).collect()
      nMains += mains.length
      val mirrors = readMirrorPartitionDF(path + "/mirrors", spark).collect()
      nMirrors += mirrors.length

      val pcPort = shardPort + numberOfShards
      val shardConfig = createConfig("shard", shardPort)
      val pcConfig = createConfig("partitionCoordinator", pcPort)

      partitionMap(pid) = shardPort
      partCoordMap(pid) = pcPort

      val entityManager = ActorSystem[EntityManager.Command](
        EntityManager(partitionMap, png.mainArray, pid, png.inEdgePartition, mains, mirrors),
        "ClusterSystem",
        shardConfig
      )
      shardActors += entityManager

      pid += 1
    }


    val frontPort = shardPorts.last + 1
    val frontRole = "front"

    val frontConfig = createConfig(frontRole, frontPort)
    val entityManager = ActorSystem[EntityManager.Command](
      EntityManager(partitionMap, png.mainArray, pid, png.inEdgePartition, null, null),
      "ClusterSystem",
      frontConfig
    )

    println("Blocking until all cluster members are up...")
    blockUntilAllMembersUp(domainListener, nNodes)

    println("Blocking until all EntityManagers are registered...")
    val emRefs: collection.mutable.Map[Int, EMRef] =
      blockUntilAllRefsRegistered[EntityManager.Command](
        entityManager,
        "entityManager",
        numberOfShards + 1
      )
    println(s"Registered ${emRefs.size} EntityManagers.")

    println("Blocking until all PartitionCoordinators are registered...")
    for (pid <- 0 until numberOfShards) {
      val emRef: EMRef = emRefs(pid)
      emRef ! EntityManager.SpawnPC(pid)
    }
    val pcRefs: collection.mutable.Map[Int, PCRef] =
      blockUntilAllRefsRegistered[PartitionCoordinator.Command](
        entityManager,
        "partitionCoordinator",
        numberOfShards
      )
    println(s"Registered ${pcRefs.size} PartitionCoordinators.")

    println("Blocking until the Global Coordinator is initialized and registered...")
    entityManager ! EntityManager.SpawnGC()
    val gcRefs: collection.mutable.Map[Int, GCRef] =
      blockUntilAllRefsRegistered[GlobalCoordinator.Command](entityManager, "globalCoordinator", 1)
    val gcRef = gcRefs(0)

    blockInitGlobalCoordinator(gcRef, entityManager.scheduler, pcRefs, nNodes)
    println(s"Registered the GlobalCoordinator.")
    println("Broadcasting the Global Coordinator address to all Partition Coordinators")
    broadcastGCtoPCs(gcRef, entityManager.scheduler)

    println("here are the em refs:")
    emRefs.foreach(println)

    for (pid <- 0 until numberOfShards) {
      val emRef = emRefs(pid)
      emRef ! InitializeMains
      emRef ! InitializeMirrors
    }


    var nMainsInitialized = 0
    var nMirrorsInitialized = 0

    for (pid <- 0 until numberOfShards) {
      val emRef: EMRef = emRefs(pid)
      nMainsInitialized += getNMainsInitialized(entityManager, emRef)
      nMirrorsInitialized += getNMirrorsInitialized(entityManager, emRef)
    }


    println("Checking that all Mains, Mirrors have been initialized...")
    println(s"Total Mains Initialized: $nMainsInitialized")
    println(s"Total Mirrors Initialized: $nMirrorsInitialized")
    assert(nMainsInitialized == nMains)
    assert(nMirrorsInitialized == nMirrors)
    return

    println(s"Initializing ${nMains} Mains and ${nMirrors} Mirrors...")
    for (main <- png.mainArray) {
      entityManager ! EntityManager.Initialize(
        VertexEntityType.Main.toString(),
        main.id,
        main.partition.id,
        main.neighbors.map(n =>
          n match {
            case neighbor: Main =>
              new EntityId(VertexEntityType.Main.toString(), neighbor.id, neighbor.partition.id)
            case neighbor: Mirror =>
              new EntityId(VertexEntityType.Mirror.toString(), neighbor.id, neighbor.partition.id)
          }
        )
      )
    }


    //     after initialization, each partition coordinator should broadcast its location to its mains
    for ((pid, pcRef) <- pcRefs) {
      println(s"PC${pid} Broadcasting location to its main")
      pcRef ! BroadcastLocation()
    }
    // ensure that the number of partition coordinator ref acknowledgements by main vertices equals the number of main
    // vertices
    var totalMainsAckd = 0
    for ((pid, pcRef) <- pcRefs) {
      val nMainsAckd = getNMainsAckd(entityManager, pcRef)
      println(s"${nMainsAckd} mains acknowledged location of PC${pid}")
      totalMainsAckd += nMainsAckd
    }
    println(s"Total Mains Acknowledged: $totalMainsAckd")
    assert(totalMainsAckd == nMains)
    gcRef ! GlobalCoordinator.BEGIN()
    // TODO at beginning send, BEGIN(0)

    // Wait until finished
    

    var finalVals: Map[Int, Option[Colour]] = null
    while(null == finalVals){
      Thread.sleep(1000)

      val timeout: Timeout = 5.seconds
      val sched = entityManager.scheduler
      val future: Future[GlobalCoordinator.FinalValuesResponse] = gcRef.ask(ref => GlobalCoordinator.GetFinalValues(ref))(timeout,sched)
      Await.result(future, Duration.Inf) match {
        case FinalValuesResponseComplete(valueMap) => {
          finalVals = valueMap
        }
        case FinalValuesResponseNotFinished => ()
      }

    }

    println("Final Values from main app:")
    for((key, value) <- finalVals){
      println(s"$key -> $value")
    }

    // TODO shut down actor system

    // increment mains and their mirrors
//    for (main <- png.mainArray)
//      entityManager ! EntityManager.AddOne(
//        VertexEntityType.Main.toString(),
//        main.id,
//        main.partition.id
//      )
//    for (main <- png.mainArray)
//      entityManager ! EntityManager.AddOne(
//        VertexEntityType.Main.toString(),
//        main.id,
//        main.partition.id
//      )

    // see if increments have been propagated correctly to mirrors
//    for (main <- png.mainArray) {
//      entityManager ! EntityManager.GetSum(
//        VertexEntityType.Main.toString(),
//        main.id,
//        main.partition.id
//      )
//      for (mirror <- main.mirrors) {
//        entityManager ! EntityManager.GetSum(
//          VertexEntityType.Mirror.toString(),
//          mirror.id,
//          mirror.partition.id
//        )
//      }
//    }
  }
}
