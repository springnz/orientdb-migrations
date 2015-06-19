name := "OrientDBScala"
version := "0.1"
scalaVersion := "2.11.6"
// val orientCommonsVersion = "2.0-M1"
val orientDBVersion = "2.1-rc4"

resolvers ++= Seq(
  Resolver.mavenLocal,
  "Orient Technologies Maven2 Repository" at "http://www.orientechnologies.com/listing/m2")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4",
  // "com.orientechnologies" % "orient-commons" % orientCommonsVersion withSources(),
  "com.orientechnologies" % "orientdb-core" % orientDBVersion withSources(),
  "com.orientechnologies" % "orientdb-graphdb" % orientDBVersion withSources(),
  "com.orientechnologies" % "orientdb-client" % orientDBVersion withSources(),
  // "com.orientechnologies" % "orientdb-enterprise" % orientDBVersion withSources(),
  // "com.tinkerpop.blueprints" % "blueprints-core" % "2.6.0"
  "com.michaelpollmeier" %% "gremlin-scala" % "3.0.0-SNAPSHOT",
  "com.michaelpollmeier" % "orientdb-gremlin" % "3.0.0-SNAPSHOT"
)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
