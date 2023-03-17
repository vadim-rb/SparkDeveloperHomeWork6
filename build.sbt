ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "SparkDeveloperHomeWork6",
      libraryDependencies ++= Seq(
    "com.nrinaudo" %% "kantan.csv-generic" % "0.7.0",
     "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % "2.13.5",
    "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.13.5",
    "org.apache.kafka" % "kafka-clients" % "3.4.0"
  ))
