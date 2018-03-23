name := "SparkStreamingWC"

version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "2.0.1",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.0.1"
)
        