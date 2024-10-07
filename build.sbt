ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.19"
val circeVersion = "0.14.4"


assembly / mainClass := Some("org.example.Job")

// make run command include the provided dependencies
Compile / run := Defaults.runTask(Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly

//add kafka stream dependency with tests and testing framework


libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka-streams-scala" % "3.8.0",
  "org.apache.kafka" % "kafka-streams-test-utils" % "3.8.0" % Test,
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % "2.30.9",
  // Use the "provided" scope instead when the "compile-internal" scope is not supported
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.30.9",
  "org.typelevel" %% "cats-core" % "2.12.0",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "io.circe" %% "circe-generic-extras" % circeVersion,
  "io.micrometer" % "micrometer-core" % "1.13.1",
  //      "org.apache.flink" %% "flink-scala" % "1.20.0",
  //      "org.apache.flink" % "flink-clients" % "1.20.0",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0",
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.16.1" % Runtime,
  "com.lmax" % "disruptor" % "3.4.4" % Runtime,
  "com.github.blemale" %% "scaffeine" % "5.3.0",
  //s3 client
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.16.1")
//"org.apache.flink" %% "flink-streaming-scala" % "1.20.0")

libraryDependencies += "software.amazon.awssdk" % "s3" % "2.28.16"
//appache commons
libraryDependencies += "org.apache.commons" % "commons-compress" % "1.26.1"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.17.0"
lazy val root = (project in file(".")).
  settings(
  )
