ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "mlproject"
  )
val spark_version = "2.4.6"
libraryDependencies += "org.apache.spark" %%  "spark-core" % spark_version
libraryDependencies += "org.apache.spark" %%  "spark-sql" % spark_version
libraryDependencies += "org.apache.spark" %%  "spark-mllib" % spark_version