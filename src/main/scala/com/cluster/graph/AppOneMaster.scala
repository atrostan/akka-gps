package com.cluster.graph

import akka.actor.typed.{ActorSystem, Behavior}
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.{Behaviors, Routers}
import akka.cluster.typed.{Cluster, ClusterSingleton, ClusterSingletonSettings, SingletonActor}
import com.typesafe.config.ConfigFactory

object AppOneMaster {

  val WorkerServiceKey = ServiceKey[VertexWorker.Process]("Worker")

  object RootBehavior {
    def apply(): Behavior[Nothing] = Behaviors.setup[Nothing] { ctx =>
      val cluster = Cluster(ctx.system)

      val singletonSettings = ClusterSingletonSettings(ctx.system)
        .withRole("compute")
      val serviceSingleton = SingletonActor(
        Behaviors.setup[VertexService.Command] { ctx =>
          // the service singleton accesses available workers through a group router
          val workersRouter =
            ctx.spawn(
              Routers
                .group(WorkerServiceKey)
                // the worker has a per word cache, so send the same word to the same worker
                .withConsistentHashingRouting(1, _.word),
              "WorkersRouter"
            )

          VertexService(workersRouter)
        },
        "StatsService"
      ).withStopMessage(VertexService.Stop)
        .withSettings(singletonSettings)
      val serviceProxy = ClusterSingleton(ctx.system).init(serviceSingleton)

      if (cluster.selfMember.hasRole("compute")) {
        // on every compute node N local workers, which a cluster singleton stats service delegates work to
        val numberOfWorkers =
          ctx.system.settings.config.getInt("vertex-service.workers-per-node")
        ctx.log.info("Starting {} workers", numberOfWorkers)
        (0 to numberOfWorkers).foreach { n =>
          val worker = ctx.spawn(VertexWorker(), s"VertexWorker$n")
          ctx.system.receptionist ! Receptionist
            .Register(WorkerServiceKey, worker)
        }
      }
      // no client in vector service
      //      if (cluster.selfMember.hasRole("client")) {
      //        ctx.spawn(StatsClient(serviceProxy), "Client")
      //      }
      Behaviors.empty
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      startup("compute", 25251)
      startup("compute", 25252)
      startup("compute", 0)
      //      startup("client", 0)
    } else {
      require(args.size == 2, "Usage: role port")
      startup(args(0), args(1).toInt)
    }
  }

  def startup(role: String, port: Int): Unit = {
    // Override the configuration of the port when specified as program argument
    val config = ConfigFactory
      .parseString(s"""
      akka.remote.artery.canonical.port=$port
      akka.cluster.roles = [$role]
      """)
      .withFallback(ConfigFactory.load("stats"))

    ActorSystem[Nothing](RootBehavior(), "ClusterSystem", config)
  }

}
