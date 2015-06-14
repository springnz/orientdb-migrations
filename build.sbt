name := "OrientDBScala"

version := "0.1"

scalaVersion := "2.11.6"

// val orientCommonsVersion = "2.0-M1"

val orientDBVersion = "2.1-rc3"

resolvers ++= Seq("Orient Technologies Maven2 Repository" at "http://www.orientechnologies.com/listing/m2")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4",
  // "com.orientechnologies" % "orient-commons" % orientCommonsVersion withSources(),
  "com.orientechnologies" % "orientdb-core" % orientDBVersion withSources(),
  "com.orientechnologies" % "orientdb-graphdb" % orientDBVersion withSources(),
  "com.orientechnologies" % "orientdb-client" % orientDBVersion withSources(),
  // "com.orientechnologies" % "orientdb-enterprise" % orientDBVersion withSources(),
  "com.tinkerpop.blueprints" % "blueprints-core" % "2.6.0"
)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
