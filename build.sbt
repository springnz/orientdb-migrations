name := "OrientDBScala"
organization := "ylabs"
scalaVersion := "2.11.7"
// val orientCommonsVersion = "2.0-M1"
val orientDBVersion = "2.1-rc4"

val repo = "https://nexus.prod.corp/content"

resolvers ++= Seq(
  "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
  "spring" at s"$repo/groups/public",
  "Orient Technologies Maven2 Repository" at "http://www.orientechnologies.com/listing/m2"
)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4",
  // "com.orientechnologies" % "orient-commons" % orientCommonsVersion withSources(),
  "com.orientechnologies" % "orientdb-core" % orientDBVersion withSources(),
  "com.orientechnologies" % "orientdb-graphdb" % orientDBVersion withSources(),
  "com.orientechnologies" % "orientdb-client" % orientDBVersion withSources(),
  // "com.orientechnologies" % "orientdb-enterprise" % orientDBVersion withSources(),
  "com.tinkerpop.blueprints" % "blueprints-core" % "2.6.0",
  "ch.qos.logback" % "logback-classic" % "1.1.3" % Compile withSources()
)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

publishTo <<= version { (v: String) ⇒
  if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at s"$repo/repositories/snapshots")
  else Some("releases" at s"$repo/repositories/releases")
}

releaseSettings
ReleaseKeys.versionBump := sbtrelease.Version.Bump.Minor
ReleaseKeys.tagName := s"${name.value}-v${version.value}"
