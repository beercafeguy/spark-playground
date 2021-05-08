name := "SparkJDBCConnect"

organization := "com.beercafeguy"
version := "0.1"

scalaVersion := "2.12.10"

autoScalaLibrary := false
val sparkVersion = "2.4.7"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  //"org.mariadb.jdbc" % "mariadb-java-client" % "2.7.2",
  "mysql" % "mysql-connector-java" % "8.0.21"
)

libraryDependencies ++= sparkDependencies
        