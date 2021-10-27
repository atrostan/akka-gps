import com.typesafe.sbt.SbtMultiJvm.multiJvmSettings
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

name := "akka-quickstart-scala"

version := "1.0"

lazy val akkaVersion = "2.6.16"
lazy val sparkVersion = "3.2.0"

lazy val `akka-gps` = project
  .in(file("."))
  .settings(multiJvmSettings: _*)
  .settings(
//    organization := "com.lightbend.akka.samples",
    scalaVersion := "2.12.15",
    Compile / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),
    Compile / javacOptions ++= Seq("-Xlint:unchecked", "-Xlint:deprecation"),
    run / javaOptions ++= Seq("-Xms128m", "-Xmx1024m", "-Djava.library.path=./target/native"),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed"            % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-typed"          % akkaVersion,
      "com.typesafe.akka" %% "akka-serialization-jackson"  % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-sharding-typed" % akkaVersion,
      "ch.qos.logback"    %  "logback-classic"             % "1.2.3",
      "com.typesafe.akka" %% "akka-multi-node-testkit"     % akkaVersion % Test,
      "org.scalatest"     %% "scalatest"                   % "3.0.8"     % Test,
      "com.typesafe.akka" %% "akka-actor-testkit-typed"    % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-persistence-typed"      % akkaVersion,
      "com.typesafe.akka" %% "akka-persistence-testkit"    % akkaVersion % Test,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.slf4j" % "slf4j-api" % "1.7.32",
      "org.slf4j" % "slf4j-simple" % "1.7.32",
      "org.clapper" %% "grizzled-slf4j" % "1.3.4",
    ),
    run / fork := false,
    Global / cancelable := false,
    // disable parallel tests
    Test / parallelExecution := false,
    licenses := Seq(("CC0", url("http://creativecommons.org/publicdomain/zero/1.0")))
  )
  .configs (MultiJvm)


