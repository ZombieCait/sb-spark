import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel

object dashboard {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("lab08")
      .getOrCreate

    val model_path = spark.conf.get("spark.mlproject.model_path")
    val input_path = spark.conf.get("spark.mlproject.input_path")

    val model = PipelineModel.load(model_path)

    val logs = spark
      .read
      .json(input_path)
      .select(col("date"), col("uid"), explode(col("visits")).alias("visits"))
      .withColumn("domain", expr("regexp_replace(parse_url(visits.url, 'HOST'), '^www.', '')"))
      .groupBy(col("date"), col("uid"))
      .agg(collect_list("domain").alias("domains"))

    val result = model.transform(logs)

    result
      .select(col("date"), col("uid"), col("label_string").alias("gender_age"))
      .write
      .format("es")
      .options(Map(
        "es.nodes" -> "10.0.0.5:9200",
        "es.batch.write.refresh" -> "false",
        "es.nodes.wan.only" -> "true",
        "es.net.http.auth.user" -> "ekaterina.patrakova",
        "es.net.http.auth.pass" -> "KOIfK1gQ"
      ))
      .save("ekaterina_patrakova_lab08/_doc")
}
}
