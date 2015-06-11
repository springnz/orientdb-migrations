name := "OrientDBScala"

version := "0.1"

scalaVersion := "2.11.6"

resolvers ++= Seq("Orient Technologies Maven2 Repository" at "http://www.orientechnologies.com/listing/m2")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4",
  "com.orientechnologies" % "orient-commons" % "1.7.10",
  "com.orientechnologies" % "orientdb-core" % "1.7.10",
  "com.orientechnologies" % "orientdb-client" % "1.7.10",
  "com.orientechnologies" % "orientdb-enterprise" % "1.7.10"
)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
