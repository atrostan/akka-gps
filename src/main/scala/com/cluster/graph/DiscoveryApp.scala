package com.cluster.graph

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.cluster.{ClusterEvent, Member}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import akka.actor.typed.scaladsl.AskPattern.Askable
import com.Typedefs.{EMRef, GCRef, PCRef}
import com.cluster.graph.ClusterShardingApp.createConfig
import com.cluster.graph.EntityManager.{InitializeMains, InitializeMirrors}
import com.cluster.graph.Init.{
  blockUntilAllRefsRegistered,
  broadcastGCtoPCs,
  getNMainsAckd,
  getNMainsInitialized,
  getNMirrorsInitialized
}
import com.cluster.graph.PartitionCoordinator.BroadcastLocation
import com.preprocessing.aggregation.HDFSUtil.getHDFSfs
import com.preprocessing.aggregation.Serialization.{
  Main,
  Mirror,
  readMainTextFile,
  readMirrorTextFile,
  readObjectArray
}
import com.preprocessing.partitioning.Util.{readMainPartitionDF, readMirrorPartitionDF}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import java.io.{File, FileOutputStream, PrintStream}
import scala.collection.mutable.ArrayBuffer

object DiscoveryApp {

  def parseArgs(args: Array[String]) = {
    // folder that contains partitioned graph;
    var partitionFolder = ""
    var myIp = ""
    var confPath = ""
    var hadoopConfDir = ""
    args.sliding(2, 2).toList.collect {
      case Array("--partitionFolder", argPartitionFolder: String) =>
        partitionFolder = argPartitionFolder
      case Array("--myIp", argMyIp: String) =>
        myIp = argMyIp
      case Array("--confPath", argConfPath: String) =>
        confPath = argConfPath
      case Array("--hadoopConfDir", argHCP: String) =>
        hadoopConfDir = argHCP
    }
    (partitionFolder, myIp, confPath, hadoopConfDir)
  }

  def parseConfigForIps(config: Config) = {
    val seedNode = config.getStringList("akka.cluster.seed-nodes").get(0)
    val seedIp = seedNode
      .split('@')(1)
      .split(':')(0)
    val partitionIps = config
      .getList("akka.partitions")
      .unwrapped()
      .toArray()
      .map(s => s.toString)
    val frontIp = config.getString("akka.front")

    (seedNode, seedIp, partitionIps, frontIp)
  }

  def blockUntilAllMembersUp(
      domainListener: ActorSystem[ClusterEvent.ClusterDomainEvent],
      nNodes: Int
  ): Unit = {
    implicit val scheduler = domainListener.scheduler

    val waitTime = 10 seconds
    implicit val timeout: Timeout = waitTime

    var flag = true
    while (flag) {
      val f: Future[ClusterMemberEventListener.nMembersUpResponse] = domainListener.ask(ref => {
        ClusterMemberEventListener.nMembersUp(ref)
      })
      val nMembersUpResponse = Await.result(f, waitTime)
      nMembersUpResponse match {
        case ClusterMemberEventListener.nMembersUpResponse(n) =>
          //          println(s"$n of ${nNodes - 1} nodes are up")
          flag = n != nNodes
        case _ =>
          flag = true
      }
      Thread.sleep(1000)
    }
  }

  def createConfig(role: String, port: Int, confPath: String): Config = {
    val config = ConfigFactory
      .parseString(s"""
      akka.remote.artery.canonical.port = $port
      akka.cluster.roles = [$role]
      """)
      .withFallback(ConfigFactory.parseFile(new File(confPath)))
    config
  }

  // runMain com.cluster.graph.DiscoveryApp --partitionFolder "./" --myIp "172.31.7.123" --confPath "/home/atrostan/Workspace/repos/AkkaDiscovery/src/main/resources/cluster.conf"

  // java -cp AkkaDiscovery.jar MainApp.DiscoveryApp --partitionFolder "./" --myIp "1.1.1.1" --confPath "/home/atrostan/Workspace/repos/AkkaDiscovery/src/main/resources/cluster.conf"

  def initSpark(): (SparkConf, SparkContext, SparkSession, Configuration) = {
    val appName: String = "akka.DiscoveryApp"
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
      .set("spark.driver.memory", "16g")
      .set("spark.driver.cores", "6")
      .set("spark.cores.max", "8")
      .set("spark.executor.cores", "6")
      .set("spark.executor.memory", "16g")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()

//    val actorSystems =
//      ArrayBuffer[ActorSystem[ClusterEvent.ClusterDomainEvent with EntityManager.Command]]()
    val hadoopConfig = sc.hadoopConfiguration
    hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    (conf, sc, spark, hadoopConfig)
  }

  def main(args: Array[String]): Unit = {
    val (partitionFolder, myIp, confPath, hadoopConfDir) = parseArgs(args)
    val canonicalPort = 25520
    val (conf, sc, spark, hadoopConfig) = initSpark()
//    val fs = getHDFSfs(hadoopConfDir)
    val fs = FileSystem.get(hadoopConfig)
    println(fs)
    println(s"myIp: ${myIp}")
    println(s"partitionFolder: ${partitionFolder}")
    println(s"confPath: ${confPath}")

    val config = ConfigFactory.parseFile(new File(confPath))
    val (seedNode, seedIp, partitionIps, frontIp) = parseConfigForIps(config)

    val numPartitions = partitionIps.length
    val partitionMap = partitionIps.zipWithIndex.map(t => (t._2, t._1)).toMap
    val invPartitionMap = (Map() ++ partitionMap.map(_.swap))

    // a map between partition ids to location on hdfs of mains, mirrors for that partition

    val workerMap = (0 until numPartitions)
      .map(i => (i, partitionFolder + s"/p$i"))
      .toMap
    val nNodes = partitionIps.length + 2

    println(s"seedNode: ${seedNode}")
    println(s"seedIp: ${seedIp}")
    println(s"partitionIps: ${partitionIps}")
    println(s"frontIp: ${frontIp}")
    println(s"nNodes: ${nNodes}")

    if (myIp == seedIp) {
      println("i am domainlistener")

      val nodesUp = collection.mutable.Set[Member]()
      val domainListenerRole = "domainListener"
      val domainListenerConfig = createConfig(domainListenerRole, canonicalPort, confPath)
      val domainListener: ActorSystem[ClusterEvent.ClusterDomainEvent] = ActorSystem(
        ClusterMemberEventListener(nodesUp, nNodes),
        "ClusterSystem",
        domainListenerConfig
      )
      println("Blocking until all cluster members are up...")
//      blockUntilAllMembersUp(domainListener, nNodes)

    } else if (partitionIps.contains(myIp)) {
      println("this is printed to console.txt")
      println(s"i am entitymanagger shard on ${myIp}")
      val myPid = invPartitionMap(myIp)

      val role = "shard"
      val path = workerMap(myPid)

//        val (conf, sc, spark) = initSpark()
//        println("here's all the conf stuffx")
//        println(conf.getAll.foreach(t => println(t._1, t._2)))
//        val mains = readMainPartitionDF(path + "/mains", spark).collect()
//val mirrors = readMirrorPartitionDF(path + "/mirrors", spark).collect()
//        println(s"reading main array from path: ${path+"/mains.ser"}")
      println(s"reading main array from path: ${path + "/mains/part-00000"}")
      val mains = readMainTextFile(path + "/mains/part-00000", fs)

      //        val mains = readObjectArray[Main](path+"/mains.ser")
      println(s"reading mirrors array from path: ${path + "/mirrors.ser"}")
      val mirrors = readMirrorTextFile(path + "/mirrors/part-00000", fs)
//        val mirrors = readObjectArray[Mirror](path+"/mirrors.ser")
      println(mains, mirrors)

      mains.foreach(println)
      mirrors.foreach(println)
      println("did we match?")
      val shardConfig = createConfig("shard", canonicalPort, confPath)
      println(collection.mutable.Map(partitionMap.toSeq: _*))
      val entityManager = ActorSystem[EntityManager.Command](
        EntityManager(
          collection.mutable.Map(partitionMap.toSeq: _*),
          myPid,
          mains,
          mirrors
        ),
        "ClusterSystem",
        shardConfig
      )
      println(s"registered em on ${myIp}")

    } else if (myIp == frontIp) {
      println("i am entitymanager front")
      val role = "front"
      val cfg = createConfig(role, canonicalPort, confPath)

      val entityManager = ActorSystem[EntityManager.Command](
        // the global entity manager is not assigned any mains, mirrors
        EntityManager(
          collection.mutable.Map(partitionMap.toSeq: _*),
          numPartitions,
          null,
          null
        ),
        "ClusterSystem",
        cfg
      )
      // can't do this in this scope, will it be an issue?
//      println("Blocking until all cluster members are up...")
//      blockUntilAllMembersUp(domainListener, nNodes)

      println("this is printed to console.txt")
      println("Blocking until all EntityManagers are registered...")
      val emRefs: collection.mutable.Map[Int, EMRef] =
        blockUntilAllRefsRegistered[EntityManager.Command](
          entityManager,
          "entityManager",
          numPartitions + 1
        )
      println(s"Registered ${emRefs.size} EntityManagers.")
      for (pid <- 0 until numPartitions) {
        val emRef: EMRef = emRefs(pid)
        emRef ! EntityManager.SpawnPC(pid)
      }
      val pcRefs: collection.mutable.Map[Int, PCRef] =
        blockUntilAllRefsRegistered[PartitionCoordinator.Command](
          entityManager,
          "partitionCoordinator",
          numPartitions
        )
      println(s"Registered ${pcRefs.size} PartitionCoordinators.")

      println("Blocking until the Global Coordinator is initialized and registered...")
      entityManager ! EntityManager.SpawnGC()
      val gcRefs: collection.mutable.Map[Int, GCRef] =
        blockUntilAllRefsRegistered[GlobalCoordinator.Command](
          entityManager,
          "globalCoordinator",
          1
        )
      val gcRef = gcRefs(0)
      broadcastGCtoPCs(gcRef, entityManager.scheduler)

      for (pid <- 0 until numPartitions) {
        val emRef = emRefs(pid)
        emRef ! InitializeMains
        emRef ! InitializeMirrors
      }

      var nMainsInitialized = 0
      var nMirrorsInitialized = 0

      for (pid <- 0 until numPartitions) {
        val emRef: EMRef = emRefs(pid)
        nMainsInitialized += getNMainsInitialized(entityManager, emRef)
        nMirrorsInitialized += getNMirrorsInitialized(entityManager, emRef)
      }
      println("Checking that all Mains, Mirrors have been initialized...")
      println(s"Total Mains Initialized: $nMainsInitialized")
      println(s"Total Mirrors Initialized: $nMirrorsInitialized")
//        assert(nMainsInitialized == nMains)
//        assert(nMirrorsInitialized == nMirrors)

      // after initialization, each partition coordinator should broadcast its location to its mains
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
//        assert(totalMainsAckd == nMains)

    }

    println("seed: ", seedNode)
    println(seedIp)
    println("front: ", frontIp)
  }
}
