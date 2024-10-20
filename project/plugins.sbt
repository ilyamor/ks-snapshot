addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")

addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.0")

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")