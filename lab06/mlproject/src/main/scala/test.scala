import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.ml.{PipelineModel}

class test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("lab07_test")
      .getOrCreate

    val modelPath = spark.conf.get("spark.mlproject.model_path")
    val kafkaInputTopic = spark.conf.get("spark.mlproject.kafka_input_topic")
    val kafkaOutputTopic = spark.conf.get("spark.mlproject.kafka_output_topic")
    val kafkaServer = "spark-master-2:6667"

    val model = PipelineModel.load(modelPath)

    val schema = StructType(Seq(
      StructField("uid", StringType, nullable = true),
      StructField("visits", ArrayType(StructType(Seq(
          StructField("url", StringType, nullable = true),
          StructField("timestamp", StringType, nullable = true)
        )))
        , nullable = true)
    ))

    val logs = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", kafkaInputTopic)
      .load

    val kafkaSink = logs
      .writeStream
      .trigger(Trigger.ProcessingTime("45 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        // Подготовка данных
        val prepared_logs = batchDF
          .select(col("timestamp"), from_json(col("value").cast("string"), schema).alias("values"))
          .select(col("values"))
          .select(col("uid"), explode(col("visits")).alias("visits"))
          .withColumn("domain",  expr("regexp_replace(parse_url(visits.url, 'HOST'), '^www.', '')"))
          .groupBy(col("uid"))
          .agg(collect_list("domain").alias("domains"))

        val result = model.transform(prepared_logs)

        result
          .select(lit(null).cast(StringType), to_json(struct(
            col("uid"),
            col("label_string").alias("gender_age")
          )).alias("value"))
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", kafkaServer)
          .option("topic", kafkaOutputTopic)
          .save
      }

    kafkaSink.start
  }
}
