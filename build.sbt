import com.typesafe.sbt.SbtMultiJvm.multiJvmSettings
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import CommandExample._

name := "akka-gps"

version := "1.0"

lazy val akkaVersion = "2.6.16"
lazy val sparkVersion = "3.1.2"

ThisBuild / assemblyMergeStrategy := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class")         => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case PathList("org", "apache", "hadoop", "yarn", "factories", "package-info.class")         => MergeStrategy.discard
  case PathList("org", "apache", "hadoop", "yarn", "provider", "package-info.class")         => MergeStrategy.discard
  case PathList("org", "apache", "hadoop", "util", "provider", "package-info.class")         => MergeStrategy.discard
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class")         => MergeStrategy.first
  case PathList("org", "aopalliance", "intercept", "MethodInvocation.class") => MergeStrategy.first
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

val myRun = taskKey[Unit]("...")

myRun := Def.taskDyn {
  val appName = name.value
  Def.task {
    (Compile / runMain)
      .toTask(s" com.softwaremill.MyMain $appName")
      .value
  }
}.value

lazy val `akka-gps` = project
  .in(file("."))
  .settings(multiJvmSettings: _*)
  .settings(
//    organization := "com.lightbend.akka.samples",
    scalaVersion := "2.12.15",
    Compile / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint", "-target:jvm-1.8"),
    Compile / javacOptions ++= Seq("-Xlint:unchecked", "-Xlint:deprecation"),
    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
    ),
    run / javaOptions ++= Seq("-Xms128m", "-Xmx8G", "-XX:+UseG1GC", "-Djava.library.path=./target/native",  "-Dlog4j.configuration=src/main/resources/log4j.properties"),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed"            % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-typed"          % akkaVersion,
      "com.typesafe.akka" %% "akka-serialization-jackson"  % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-sharding-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-multi-node-testkit"     % akkaVersion % Test,
      "org.scalatest"     %% "scalatest"                   % "3.0.8"     % Test,
      "com.typesafe.akka" %% "akka-actor-testkit-typed"    % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-persistence-typed"      % akkaVersion,
      "com.typesafe.akka" %% "akka-persistence-testkit"    % akkaVersion % Test,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.scala-graph" %% "graph-core" % "1.12.5"
    ),
    run / fork := false,
    Global / cancelable := false,
    // disable parallel tests
    Test / parallelExecution := false,
    licenses := Seq(("CC0", url("http://creativecommons.org/publicdomain/zero/1.0"))),
    commands ++= Seq(hello, changeColor, partitionBySource1D)

  )
  .configs (MultiJvm)



