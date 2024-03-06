ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"


lazy val root = (project in file("."))
  .settings(
    name := "SessionCounter"
  )


val flinkVersion = "1.18.1"

libraryDependencies += "org.apache.flink" %% "flink-scala" % flinkVersion
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-clients" % flinkVersion
//libraryDependencies += "org.apache.flink" % "flink-connector-kafka" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-connector-kafka" % "3.1.0-1.18"
//libraryDependencies += "org.apache.flink" % "flink-avro" % flinkVersion
libraryDependencies += "org.apache.avro" % "avro" % "1.8.2"

