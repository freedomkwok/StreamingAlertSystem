ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "SessionCounter"
  )


libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.10.1"
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.10.1"
libraryDependencies += "org.apache.flink" %% "flink-connector-kafka" % "1.10.1"
libraryDependencies += "org.apache.flink" % "flink-avro" % "1.10.1"
libraryDependencies += "org.apache.avro" % "avro" % "1.8.2"

