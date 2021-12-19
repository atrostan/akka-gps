import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import com.typesafe.sbt.SbtMultiJvm.multiJvmSettings

name := "akka-gps"

version := "1.0"

lazy val akkaVersion = "2.6.16"
lazy val sparkVersion = "3.1.2"
val AkkaManagementVersion = "1.1.1"
val AkkaHttpVersion = "10.2.7"

val meta = """META.INF(.)*""".r
ThisBuild / assemblyMergeStrategy := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.contains("services")                   => MergeStrategy.concat
  case n if n.startsWith("reference.conf")           => MergeStrategy.concat
  case n if n.endsWith(".conf")                      => MergeStrategy.concat
  case meta(_)                                       => MergeStrategy.discard
  case x                                             => MergeStrategy.first
}

lazy val `akka-gps` = project
  .in(file("."))
  .settings(multiJvmSettings: _*)
  .settings(
    //    organization := "com.lightbend.akka.samples",
    scalaVersion := "2.12.15",
    Compile / scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlog-reflective-calls",
      "-Xlint",
      "-target:jvm-1.8"
    ),
    Compile / javacOptions ++= Seq("-Xlint:unchecked", "-Xlint:deprecation"),
    // Compile / PB.targets := Seq(
    //   scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
    // ),
    run / javaOptions ++= Seq(
      "-Xms128m",
      "-Xmx8G",
      "-XX:+UseG1GC",
      "-Djava.library.path=./target/native",
      "-Dlog4j.configuration=src/main/resources/log4j.properties"
    ),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.2",
      "com.typesafe.akka" %% "akka-cluster-sharding-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % Test,
      "org.scalatest" %% "scalatest" % "3.0.8" % Test,
      "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-persistence-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-persistence-testkit" % akkaVersion % Test,
      "org.apache.spark" %% "spark-core" % sparkVersion,
//      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
//      "org.apache.spark" %% "spark-avro" % sparkVersion,
      "org.yaml" % "snakeyaml" % "1.29",
      "org.apache.hadoop" % "hadoop-hdfs" % "3.3.1",
      "org.scala-graph" %% "graph-core" % "1.12.5",
//      "com.typesafe.akka" % "akka-slf4j_2.11" % "2.5.32",
      "ch.qos.logback" % "logback-classic" % "1.2.7",
    ),
    run / fork := false,
    Global / cancelable := false,
    // disable parallel tests
    Test / parallelExecution := false,
    licenses := Seq(("CC0", url("http://creativecommons.org/publicdomain/zero/1.0"))),
    assembly / assemblyJarName := "akka-gps.jar"
//    assembly / mainClass := Some("com.preprocessing.edgeList.Driver"),
  )
  .configs(MultiJvm)
