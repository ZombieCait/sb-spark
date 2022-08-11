import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object filter{
  def main(args: Array[String]): Unit = {

    var spark = SparkSession.builder
      .appName("Lab04a_patrakova")
      .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val offset: String = spark.sparkContext.getConf.get("spark.filter.offset")
    println(offset)
    val subscribe: String = spark.sparkContext.getConf.get("spark.filter.topic_name")
    println(subscribe)
    val output_dir_prefix: String = spark.sparkContext.getConf.get("spark.filter.output_dir_prefix")
    println(output_dir_prefix)

    val offset_kafka = if (offset.contains("earliest")) offset else  "{\"" + subscribe + "\":{\"0\":" + offset + "}}"
    println(offset_kafka)

    val kafka_options = Map("subscribe" -> subscribe,
                            "kafka.bootstrap.servers" -> "spark-master-1:6667",
                            "startingOffsets" -> offset_kafka
                            )

    var df = spark.read.format("kafka").options(kafka_options).load()

    val columns = Seq("event_type", "category", "item_id", "item_price", "uid", "timestamp_val")
    df = df.select(
      json_tuple(col("value").cast("string"), "event_type", "category",
        "item_id", "item_price",
        "uid", "timestamp").as(columns),
      date_format(from_unixtime(col("timestamp_val")/1000), "yyyyMMdd").as("date"),
      date_format(from_unixtime(col("timestamp_val")/1000), "yyyyMMdd").as("_date")
    )

    var view = df.where(col("event_type")==="view")
    var buy = df.where(col("event_type")==="buy")

    view.write
      .partitionBy("_date")
      .format("json")
      .mode("overwrite")
      .option("path", output_dir_prefix + "/view")
      .save()

    buy.write
      .partitionBy("_date")
      .format("json")
      .mode("overwrite")
      .option("path", output_dir_prefix + "/buy")
      .save()
  }
}
