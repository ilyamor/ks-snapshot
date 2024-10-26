ThisBuild / version := "0.1.10"
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / organization := "io.github.ilyamor"
ThisBuild / organizationName := "ks-snapshot"
ThisBuild / organizationHomepage := Some(url("https://github.com/ilyamor/ks-snapshot/"))

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/ilyamor/ks-snapshot/"),
    "scm:git@github.com:ilyamor/ks-snapshot.git"
  )
)

ThisBuild / developers := List(
  Developer(
    id = "ilyamor",
    name = "ilyamor",
    email = "ilyamor12@gmail.com",
    url = url("https://github.com/ilyamor/")
  ),
  Developer(
    id = "grinfeld",
    name = "grinfeld",
    email = "grinfeld@gmail.com",
    url = url("https://github.com/grinfeld/")
  )
)

ThisBuild / licenses := List(
  "Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")
)
ThisBuild / homepage := Some(url("https://github.com/ilyamor/ks-snapshot/"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials_s01")

ThisBuild / publishTo := {
  // For accounts created after Feb 2021:
  val nexus = "https://s01.oss.sonatype.org/"
  //val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

ThisBuild / publishMavenStyle := true

ThisBuild / scalaVersion := "2.13.14"

val versions = new {
  val circe = "0.14.4"
  val testContainers = "1.20.2"
  val jsoniterScala = "2.30.9"
  val log4j = "2.23.1"
  val jackson = "2.17.2"
}
name := "ks-snapshot"
organization := "io.github.ilyamor"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka-streams-scala" % "3.8.0" % Provided, // should be provided
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % versions.jsoniterScala,
  // Use the "provided" scope instead when the "compile-internal" scope is not supported
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % versions.jsoniterScala,
  "org.typelevel" %% "cats-core" % "2.12.0",
  "org.slf4j" % "slf4j-api" % "2.0.16" % Provided,
  "org.apache.logging.log4j" %% "log4j-api-scala" % "13.1.0" % Provided,
  "org.apache.logging.log4j" % "log4j-core" % versions.log4j % Provided,
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % versions.log4j % Provided,
  "com.fasterxml.jackson.core" % "jackson-databind" % versions.jackson % Provided,
  "com.lmax" % "disruptor" % "3.4.4" % Runtime,
  "software.amazon.awssdk" % "s3" % "2.28.16" % Provided, // should be provided
  "org.apache.commons" % "commons-compress" % "1.26.1",
  "org.apache.commons" % "commons-lang3" % "3.17.0",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "org.testcontainers" % "kafka" % versions.testContainers % Test,
  "org.testcontainers" % "minio" % versions.testContainers % Test,
  "io.minio" % "minio-admin" % "8.5.12" % Test
)

lazy val root = (project in file(".")).settings(
  name := "ks-snapshot"
)
