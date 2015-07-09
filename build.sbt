name := "orientdb-lib"
organization := "ylabs"
scalaVersion := "2.11.7"
val orientDBVersion = "2.1-rc5"

val repo = "https://nexus.prod.corp/content"
resolvers ++= Seq(
  Resolver.mavenLocal,
  "Orient Technologies Maven2 Repository" at "http://www.orientechnologies.com/listing/m2",
  "spring" at s"$repo/groups/public"
)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % Test,
  "ylabs" %% "util-lib" % "1.2.0",
  "com.typesafe" % "config" % "1.3.0",
  "com.orientechnologies" % "orientdb-core" % orientDBVersion,
  "com.orientechnologies" % "orientdb-graphdb" % orientDBVersion,
  "com.orientechnologies" % "orientdb-client" % orientDBVersion,
  "com.michaelpollmeier" %% "gremlin-scala" % "3.0.0.M9-incubating",
  "com.michaelpollmeier" % "orientdb-gremlin" % "3.0.0.M1"
    // "com.orientechnologies" % "orientdb-enterprise" % orientDBVersion,
    // "com.tinkerpop.blueprints" % "blueprints-core" % "2.6.0"
)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

publishTo <<= version { (v: String) ⇒
  if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at s"$repo/repositories/snapshots")
  else Some("releases" at s"$repo/repositories/releases")
}

releaseSettings
ReleaseKeys.versionBump := sbtrelease.Version.Bump.Minor
ReleaseKeys.tagName := s"${name.value}-v${version.value}"
