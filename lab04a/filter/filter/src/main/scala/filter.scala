import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object filter {
  def main(args: Array[String]): Unit = {
    val user_name = "ekaterina_patrakova"
    val password = "KOIfK1gQ"
    var spark = SparkSession.builder
      .appName("Lab04a_patrakova")
      .getOrCreate()

    val offset: String = spark.sparkContext.getConf.get("spark.filter.offset")
    val subscribe: String = spark.sparkContext.getConf.get("spark.filter.topic_name")
    val output_dir_prefix: String = spark.sparkContext.getConf.get("spark.filter.output_dir_prefix")

    val path = if (output_dir_prefix.contains("user")) output_dir_prefix else  "/user/ekaterina.patrakova/"+output_dir_prefix
    val offset_kafka = if (offset.contains("earliest")) offset else  "{\"" + subscribe + "\":{\"0\":" + offset + "}}"

    val kafka_options = Map("subscribe" -> subscribe,
                            "kafka.bootstrap.servers" -> "spark-master-1:6667",
                            "startingOffsets" -> offset_kafka
                            )

    var df = spark.read.format("kafka").options(kafka_options).load()

    val columns = Seq("event_type", "category", "item_id", "item_price", "uid", "timestamp_val")
    df = df.select(col("key"),
      col("topic"),
      col("partition"),
      col("offset"),
      col("timestamp"),
      col("timestampType"),
      json_tuple(col("value").cast("string"), "event_type", "category",
        "item_id", "item_price",
        "uid", "timestamp").as(columns),
      date_format(from_unixtime(col("timestamp_val")/1000), "yyyyMMdd").as("date"),
      date_format(from_unixtime(col("timestamp_val")/1000), "yyyyMMdd").as("date_p")
    )

    var view = df.where(col("event_type")==="view")
    var buy = df.where(col("event_type")==="buy")

    view = view.select(col("key"),
      to_json(struct(col("event_type"), col("category"), col("item_id"),
        col("item_price"), col("uid"), col("timestamp_val").as("timestamp"), col("date"))).as("value"),
      col("topic"),
      col("partition"),
      col("offset"),
      col("timestamp"),
      col("timestampType"),
      col("date_p"))

    view.write
      .partitionBy("date_p")
      .format("json")
      .option("path", path + "/view")
      .save()


    buy = buy.select(col("key"),
      to_json(struct(col("event_type"), col("category"), col("item_id"),
        col("item_price"), col("uid"), col("timestamp_val").as("timestamp"), col("date"))).as("value"),
      col("topic"),
      col("partition"),
      col("offset"),
      col("timestamp"),
      col("timestampType"),
      col("date_p"))

    buy.write
      .partitionBy("date_p")
      .format("json")
      .option("path", path + "/buy")
      .save()
  }
}
