name := "orientdb-migrations"
organization := "springnz"
scalaVersion := "2.11.8"

fork := true // if OrientDb version > 2.1-RC5

val repo = "https://nexus.prod.corp/content"
resolvers ++= Seq(
  Resolver.mavenLocal,
  "Orient Technologies Maven2 Repository" at "http://www.orientechnologies.com/listing/m2",
  "Sonatype OSS" at "https://oss.sonatype.org/content/repositories/public",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "spring" at s"$repo/groups/public"
)

libraryDependencies ++= Seq(
  "springnz" %% "util-lib" % "2.10.0",
  "org.scalaz" %% "scalaz-core" % "7.1.3",
  "com.typesafe" % "config" % "1.3.0",
  "com.orientechnologies" % "orientdb-client" % "2.1.11",
  "org.scalatest" %% "scalatest" % "2.2.5"
)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

publishTo <<= version { (v: String) ⇒
  if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at s"$repo/repositories/snapshots")
  else Some("releases" at s"$repo/repositories/releases")
}

releaseSettings
ReleaseKeys.versionBump := sbtrelease.Version.Bump.Minor
ReleaseKeys.tagName := s"${name.value}-v${version.value}"
