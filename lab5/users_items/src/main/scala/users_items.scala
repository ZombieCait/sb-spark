import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object users_items {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("patrakova_lab05")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val input_dir = spark.sparkContext.getConf.get("spark.users_items.input_dir")
    println(input_dir)
    val output_dir = spark.sparkContext.getConf.get("spark.users_items.output_dir")
    println(output_dir)
    val update: String = spark.sparkContext.getConf.get("spark.users_items.update")
    println(update)

    val get_item_name = regexp_replace(lower(col("item_id")), " |-", "_")

    val views = spark
      .read
      .json(input_dir + "/view")
      .withColumn("item_id", concat(lit("view_"), get_item_name))

    val buys = spark
      .read
      .json(input_dir + "/buy")
      .withColumn("item_id", concat(lit("buy_"), get_item_name))

    val union = views.union(buys)
    val max_dt = union
      .select(expr("""max(from_unixtime(timestamp_val/1000), "yyyyMMdd")"""))
      .collect()(0)(0)

    var user_items = union
      .groupBy(col("uid"))
      .pivot("item_id")
      .count
      .na.fill(0)

    if (update.contains("1")) {
      val old_user_items = spark.read.parquet(s"$output_dir/20200429")

      val left_сols = user_items.columns.toList
      val right_сols = old_user_items.columns.toList

      right_сols.diff(left_сols).foreach(x => user_items = user_items.withColumn(x, lit(0)))

      user_items = old_user_items.union(user_items.select(right_сols.head, right_сols.tail: _*))
        .groupBy("uid").sum().toDF(right_сols: _*)
    }

    user_items
      .write
      .parquet(s"$output_dir/$max_dt")

  }
}
