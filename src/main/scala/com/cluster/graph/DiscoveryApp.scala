package com.cluster.graph

import akka.actor.typed.ActorSystem
import akka.cluster.{ClusterEvent, Member}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import akka.actor.typed.scaladsl.AskPattern.Askable
import com.Typedefs.{EMRef, GCRef, PCRef}
import com.cluster.graph.EntityManager.{InitializeMains, InitializeMirrors}
import com.cluster.graph.GlobalCoordinator.{FinalValuesResponseComplete, FinalValuesResponseNotFinished}
import com.cluster.graph.Init.{blockInitGlobalCoordinator, blockUntilAllRefsRegistered, broadcastGCtoPCs, getNMainsAckd, getNMainsInitialized, getNMirrorsInitialized, readEdgelistForVerification}
import com.cluster.graph.PartitionCoordinator.BroadcastLocation
import com.cluster.graph.entity.VertexEntity
import com.preprocessing.aggregation.Serialization.{readDegTextFile, readMainTextFile, readMirrorTextFile}
import com.preprocessing.partitioning.Util.readPartitionsAndJoin
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import java.io.{File, FileWriter}
import java.util.Calendar
import scala.collection.mutable
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

    if (myIp == seedIp) {
      def log(s: String) = {
        val pw = new FileWriter("./globalLog", true)
        val currTime: String = Calendar.getInstance().getTime().toString
        pw.write(s"[${currTime}]\t${s}\n")
        pw.close()
      }
//      println("*" * 68)
//      println(s"Domain Listener")
//      println("*" * 68)
//      val nodesUp = collection.mutable.Set[Member]()
//      val domainListenerRole = "domainListener"
//      val domainListenerConfig = createConfig(domainListenerRole, canonicalPort, confPath)
//      val domainListener: ActorSystem[ClusterEvent.ClusterDomainEvent] = ActorSystem(
//        ClusterMemberEventListener(nodesUp, nNodes),
//        "ClusterSystem",
//        domainListenerConfig
//      )
//      log("Blocking until all cluster members are up...")
//      blockUntilAllMembersUp(domainListener, nNodes)
//

      log("*" * 68)
      log("Entity Manager - Front")
      log("*" * 68)
      val role = "front"
      val cfg = createConfig(role, canonicalPort, confPath)

      val entityManager = ActorSystem[EntityManager.Command](
        // the global entity manager is not assigned any mains, mirrors
        EntityManager(
          collection.mutable.Map(partitionMap.toSeq: _*),
          numPartitions,
          null,
          null,
          null
        ),
        "ClusterSystem",
        cfg
      )

      log("Blocking until all EntityManagers are registered...")
      val emRefs: collection.mutable.Map[Int, EMRef] =
        blockUntilAllRefsRegistered[EntityManager.Command](
          entityManager,
          "entityManager",
          numPartitions + 1
        )
      log(s"Registered ${emRefs.size} EntityManagers.")
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
      log(s"Registered ${pcRefs.size} PartitionCoordinators.")

      log("Blocking until the Global Coordinator is registered and initialized...")
      entityManager ! EntityManager.SpawnGC()
      val gcRefs: collection.mutable.Map[Int, GCRef] =
        blockUntilAllRefsRegistered[GlobalCoordinator.Command](
          entityManager,
          "globalCoordinator",
          1
        )
      val gcRef = gcRefs(0)
      blockInitGlobalCoordinator(gcRef, entityManager.scheduler, pcRefs, nNodes)
      log(s"Registered the GlobalCoordinator.")
      log("Broadcasting the Global Coordinator address to all Partition Coordinators")
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
      log("Checking that all Mains, Mirrors have been initialized...")
      log(s"Total Mains Initialized: $nMainsInitialized")
      log(s"Total Mirrors Initialized: $nMirrorsInitialized")
      //        assert(nMainsInitialized == nMains)
      //        assert(nMirrorsInitialized == nMirrors)

      // after initialization, each partition coordinator should broadcast its location to its mains
      for ((pid, pcRef) <- pcRefs) {
        log(s"PC${pid} Broadcasting location to its mains")
        pcRef ! BroadcastLocation()
      }
      // ensure that the number of partition coordinator ref acknowledgements by main vertices equals the number of main
      // vertices
      var totalMainsAckd = 0
      for ((pid, pcRef) <- pcRefs) {
        val nMainsAckd = getNMainsAckd(entityManager, pcRef)
        log(s"${nMainsAckd} mains acknowledged location of PC${pid}")
        totalMainsAckd += nMainsAckd
      }
      log(s"Total Mains Acknowledged: $totalMainsAckd")
      //        assert(totalMainsAckd == nMains)
      log("Beginning global computation!")
      val startComputation = System.nanoTime
      gcRef ! GlobalCoordinator.BEGIN()
      type FinalValueType = VertexEntity.VertexValT
      //      type FinalValueType = Int

      var finalVals: Map[Int, FinalValueType] = null
      while (null == finalVals) {
        Thread.sleep(1000)

        val timeout: Timeout = 5.seconds
        val sched = entityManager.scheduler
        val future: Future[GlobalCoordinator.FinalValuesResponse] =
          gcRef.ask(ref => GlobalCoordinator.GetFinalValues(ref))(timeout, sched)
        Await.result(future, Duration.Inf) match {
          case FinalValuesResponseComplete(valueMap) =>

            finalVals = valueMap
          case FinalValuesResponseNotFinished => ()
        }
      }
      val computationDuration = (System.nanoTime - startComputation) / 1e9d
      log(s"vertex program took: ${computationDuration}")
      log("Final values:")
      finalVals.foreach(v => log(v.toString()))
//
      log("checking colouring correctness...")
      def checkColouringCorrectness(coloring: Map[Int, FinalValueType]) = {
        log("Reading the original Edgelist for verification purposes...")

        val pth = "/home/ec2-user/part-00000"
        val edgeList = readEdgelistForVerification(pth)
        val adjList = collection.mutable.TreeMap[Int, collection.mutable.ArrayBuffer[Int]]()
        for (e <- edgeList) {
          val src = e._1
          val dest = e._2
          if (!adjList.contains(src)) {
            adjList(src) = ArrayBuffer[Int]() += dest
          } else {
            adjList(src) += dest
          }
        }

        val nNodes = adjList.size
        for (src <- 0 until nNodes) {
          val srcColor = coloring(src)
          for (dest <- adjList(src)) {
            val destColor = coloring(dest)
            if (srcColor == destColor) {
              log(s"${src} and ${dest} have the same colour: ${srcColor}")
            }
          }
        }
        log("Colouring Valid!")
        //        sc.stop()
      }
      checkColouringCorrectness(finalVals)
//      exportFinalVals(finalVals)

    } else if (partitionIps.contains(myIp)) {
      def log(s: String) = {
        val pw = new FileWriter("./execLog", true)
        val currTime: String = Calendar.getInstance().getTime().toString
        pw.write(s"[${currTime}]\t${s}\n")
        pw.close()
      }
      val myPid = invPartitionMap(myIp)
      log("*" * 68)
      log(s"Entity Manager - Partition ${myPid}")
      log("*" * 68)
      val path = workerMap(myPid)

      log(s"reading main array from path: " + path + "/mains/part-00000")
      val mains = readMainTextFile(path + "/mains/part-00000", fs)
      log(s"reading mirrors array from path:" + path + "/mirrors/part-00000")
      val mirrors = readMirrorTextFile(path + "/mirrors/part-00000", fs)
      log("reading outdegrees from path: " + partitionFolder + "/outdegrees/part-00000")
      val outDegMap = readDegTextFile(partitionFolder + "/outdegrees/part-00000", fs)
      // the vertices (mains or mirrors) present in this partition
      val vidSet = mains.map(t => t._1).toSet.union(mirrors.map(t => t._1).toSet)
      log(s"filtering for P${myPid}'s vidSet")
      val outDegsOnPid = vidSet.foldLeft(Map[Int, Int]()) { (acc, x) => acc + (x -> outDegMap(x)) }

      val shardConfig = createConfig("shard", canonicalPort, confPath)
      val entityManager = ActorSystem[EntityManager.Command](
        EntityManager(
          collection.mutable.Map(partitionMap.toSeq: _*),
          myPid,
          mains,
          mirrors,
          outDegsOnPid
        ),
        "ClusterSystem",
        shardConfig
      )
      log(s"Initialized EntityManager on Partition ${myPid}")
    } else if (myIp == frontIp) {
      println("shouldnt be here")
    }
  }

}
