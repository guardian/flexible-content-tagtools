name := "tag-differ"

version := "1.0"

fork := true

scalaVersion := "2.11.4"

javaOptions in run += "-Xmx12G"

resolvers ++= Seq(
    "Guardian GitHub Repository" at "https://guardian.github.io/maven/repo-releases",
    "typesafe" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= Seq(
    "com.gu" %% "management" % "5.35",
    "joda-time" % "joda-time" % "2.3",
    "com.oracle" % "jdbc_11g" % "11.2.0.3.0",
    "com.gu" %% "configuration" % "4.0",
    "org.reactivemongo" %% "reactivemongo" % "0.10.5.0.akka23",
    "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test",
    "org.scalaz" %% "scalaz-core" % "7.1.0",
    "com.typesafe.play" %% "play-json" % "2.3.6",
    "org.json4s" %% "json4s-native" % "3.3.0"
)
