import org.elasticsearch.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.SparkSession

def get_age_category =  udf { (age: Int) =>
  age match {
    case x if ((x>=18) & (x<=24)) => "18-24"
    case x if ((x>=25) & (x<=34)) => "25-34"
    case x if ((x>=35) & (x<=44)) => "35-44"
    case x if ((x>=45) & (x<=54)) => "45-54"
    case _ => ">=55"
  }
}

def get_domain =  udf { (str:String) => {
  str.split("/")(0)
  }
}

object data_mart{
  val user_name = "ekaterina_patrakova"
  val password = "KOIfK1gQ"
  var spark =  SparkSession.builder.master("yarn").appName("Lab3_patrakova")

  spark.conf.set("spark.cassandra.connection.host", "10.0.0.31")
  spark.conf.set("spark.cassandra.connection.port", "9042")
  spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
  spark.conf.set("spark.cassandra.input.consistency.level", "ONE")

  //Visits from ElasticSearch
  val esOptions =
    Map(
      "es.nodes" -> "10.0.0.31:9200",
      "es.batch.write.refresh" -> "false",
      "es.nodes.wan.only" -> "true"
    )

  var visits = spark.read.format("es").options(esOptions).load("visits")
    .withColumn("shop",
      concat(lit("shop_"),
        regexp_replace(lower(col("category"))," |-", "_")))

  visits = visits.groupBy("uid").pivot("shop").count()

  //Clients from cassandra
  var clients = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "clients", "keyspace" -> "labdata"))
    .load()
    .withColumn("age_cat", get_age_category(col("age")))

  //Categories from PostgreSQL
  val websites_categories = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
    .option("dbtable", "domain_cats")
    .option("user", user_name)
    .option("password", password)
    .option("driver", "org.postgresql.Driver")
    .load()
    .withColumn("category", concat(lit("web_"), col("category")))

  // Weblogs from hdfs
  var weblogs = spark.read.json("hdfs:///labs/laba03/weblogs.json")
    .select(col("uid"), explode(col("visits")("url")).as("URL"))
    .withColumn("domain", regexp_replace(get_domain(regexp_replace(col("URL"),
      "^(https://)|(http://)", "")), "^(www.)", ""))

  weblogs = weblogs.join(websites_categories, Seq("domain"), "left")
                   .groupBy("uid").pivot("category").count()

  var result = clients.join(visits, Seq("uid"), "left")
    .join(weblogs,  Seq("uid"), "left")
    .drop("age", "null")

  //write result
  result.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.0.31:5432/"+user_name)
    .option("dbtable", "clients")
    .option("user", user_name)
    .option("password", password)
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
}