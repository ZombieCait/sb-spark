ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "dashboard"
  )

val spark_version = "2.4.6"

libraryDependencies += "org.apache.spark" %%  "spark-core" % spark_version
libraryDependencies += "org.apache.spark" %%  "spark-sql" % spark_version
libraryDependencies += "org.apache.spark" %%  "spark-mllib" % spark_version
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "6.8.9"