import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._

object agg {
  def main(args: Array[String]): Unit = {
    val server = "spark-master-1:6667"
    val topic_in = "ekaterina_patrakova"
    val topic_out = "ekaterina_patrakova_lab04b_out"

    val schema = StructType(Seq(
      StructField("event_type", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("item_id", StringType, nullable = true),
      StructField("item_price", IntegerType, nullable = true),
      StructField("uid", StringType, nullable = true),
      StructField("timestamp", LongType, nullable = true)
    ))

    val spark = SparkSession
      .builder
      .appName("patrakova_lab04b")
      .getOrCreate

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", server)
      .option("subscribe", topic_in)
      .option("failOnDataLoss", value = false)
      .load
      .withColumn("values", from_json(col("value").cast("string"), schema))
      .select(col("values.*"))
//      .dropDuplicates(Seq("uid", "timestamp"))
      .groupBy(window(to_timestamp(col("timestamp") / 1000), "1 hour", "1 hour"))
      .agg(
        expr("sum(case when event_type='buy' then item_price else 0 END)").as("revenue"),
        count(col("uid")).as("visitors"),
        expr("sum(case when event_type='buy' then 1 else 0 END)").as("purchases")
      )
      .select(lit(null).cast(StringType), to_json(struct(
        col("window.start").cast(LongType).as("start_ts"),
        col("window.end").cast(LongType).as("end_ts"),
        col("revenue"),
        col("visitors"),
        col("purchases"),
        (col("revenue") / col("purchases")).as("aov")
      )).as("value"))

    val sink = df
      .writeStream
      .format("kafka")
      .outputMode("update")
      .option("kafka.bootstrap.servers", server)
      .option("topic", topic_out)
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .option("checkpointLocation", "chk/chunk_0")
      .start
      .awaitTermination
  }
}
