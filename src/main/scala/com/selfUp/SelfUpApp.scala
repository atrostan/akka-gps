package com.selfUp

import akka.actor.typed.scaladsl.{Behaviors, Routers}
import akka.actor.typed.{ActorSystem, Behavior}
import akka.cluster.typed.{Cluster, SelfUp, Subscribe}
import com.typesafe.config.ConfigFactory
import com.selfUp.ClusterDomainEventListener

object SelfUpApp {

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      startup("domainListener", 25251)
      startup("director", 25252)
      startup("aggregator", 25253)
      startup("worker", 25254)

    } else {
      require(
        args.size == 2,
        "Usage: two params required 'role' and 'port'")
      startup(args(0), args(1).toInt)
    }
  }

  def startup(role: String, port: Int): Unit = {

    val config = ConfigFactory
      .parseString(
        s"""
      akka.remote.artery.canonical.port=$port
      akka.cluster.roles = [$role]
      """)
      .withFallback(ConfigFactory.load("words"))

    if (role == "domainListener") {
//      ActorSystem(ClusterMemberEventListener(), "WordsCluster", config)
    } else {
      ActorSystem(ClusteredGuardian(), "WordsCluster", config)
//      ActorSystem(ClusterGuardian(), "WordsCluster", config)
    }

  }

  private object ClusterGuardian {
    def apply(): Behavior[SelfUp] =
    Behaviors.setup[SelfUp](context => {
      val cluster = Cluster(context.system)
      cluster.subscriptions ! Subscribe(context.self, classOf[SelfUp])
      import akka.actor.typed.scaladsl.adapter._
//      val sysListener = ctx.spawn(SystemListener(), "sysListener")
//      ctx.system.toClassic.eventStream.subscribe(sysListener.toClassic, classOf[akka.actor.DeadLetter])

      Behaviors.receiveMessage {
        case SelfUp(_) =>
//          val resultTopicActor = ctx.spawn(Topic[WorkFinished]("work-result"), "WorkResultTopic")

          context.log.info("{} Node is up", cluster.selfMember.roles)

          if (cluster.selfMember.hasRole("back-end")) {
//            WorkManagerSingleton.init(ctx.system, resultTopicActor)
          }

          if (cluster.selfMember.hasRole("front-end")) {
//            val workManagerProxy = WorkManagerSingleton.init(ctx.system, resultTopicActor)
//            ctx.spawn(FrontEnd(workManagerProxy, resultTopicActor), "front-end")
          }

          if (cluster.selfMember.hasRole("worker")) {
//            (1 to workers).foreach(n => ctx.spawn(Worker(resultTopicActor), s"worker-$n"))
          }

          if (cluster.selfMember.hasRole("director")) {
            context.log.info("director branch")

            // cluster.registerOnMemberUp
            Cluster(context.system).subscriptions ! Subscribe(
              context.self,
              classOf[SelfUp])
          }

          if (cluster.selfMember.hasRole("aggregator")) {
            context.log.info("aggregator branch")
            val numberOfWorkers =
              context.system.settings.config
                .getInt("example.cluster.workers-per-node")
            for (i <- 0 to numberOfWorkers) {
              //with supervision resume
              context.spawn(Worker(), s"worker-$i")
            }
          }

          Behaviors.same
      }
    })
  }
//  https://discuss.lightbend.com/t/whats-the-typed-story-of-cluster-registeronmemberup/847/3
  private object ClusteredGuardian {

    def apply(): Behavior[SelfUp] =
      Behaviors.setup[SelfUp] { context =>

        val cluster = Cluster(context.system)
        if (cluster.selfMember.hasRole("director")) {
          context.log.info("director branch")

          // cluster.registerOnMemberUp
          Cluster(context.system).subscriptions ! Subscribe(
            context.self,
            classOf[SelfUp])
        }
        if (cluster.selfMember.hasRole("aggregator")) {
          context.log.info("aggregator branch")
          val numberOfWorkers =
            context.system.settings.config
              .getInt("example.cluster.workers-per-node")
          for (i <- 0 to numberOfWorkers) {
            //with supervision resume
            context.spawn(Worker(), s"worker-$i")
          }
        }
        context.log.info("receiving msgs")

        Behaviors.receiveMessage {
          case SelfUp(_) =>
            context.log.info("someone is up")
            val router = context.spawnAnonymous {
              Routers
                .group(Worker.RegistrationKey)
            }
            context.spawn(Master(router), "master")
            Behaviors.same
        }
      }
  }
}
