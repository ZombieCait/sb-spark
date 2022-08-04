ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.12"
val sparkVersion = "2.4.5"

lazy val root = (project in file("."))
  .settings(
    name := "data_mart"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "6.8.9",
  "org.postgresql" % "postgresql" % "42.2.12"
)