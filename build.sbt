name := "triest"

version := "1.0"

scalaVersion := "2.11.8"

val flinkVersion = "1.3.2"

val flinkDependencies: Seq[ModuleID] = Seq(
  "org.apache.flink" %% "flink-scala",
  "org.apache.flink" %% "flink-clients",
  "org.apache.flink" %% "flink-streaming-scala"
).map(_ % flinkVersion)

libraryDependencies ++= flinkDependencies
