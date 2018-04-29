name := "SparkRevenueCalculation-1.0"

version := "1.0"

scalaVersion := "2.10.5"

val sparkVersion = "1.6.0"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
        