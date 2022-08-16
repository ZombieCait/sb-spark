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
    val update = spark.sparkContext.getConf.get("spark.users_items.update")
    println(update)

    val get_item_name = regexp_replace(lower(col("item_id"))," |-", "_")

    val views = spark
      .read
      .json(input_dir + "/view")

    val buys = spark
      .read
      .json(input_dir + "/buy")

    val max_dt = views.union(buys)
      .select(expr("max(date)"))
      .collect()(0)(0)
    val views_pivot = views
      .groupBy(col("uid"))
      .pivot(concat(lit("view_"), get_item_name))
      .count

    val buy_pivot = buys
      .groupBy(col("uid"))
      .pivot(concat(lit("buy_"), get_item_name))
      .count

    var user_items = views_pivot.union(buy_pivot)
    user_items = user_items.na.fill(0, user_items.columns)

    if (update == "1") {
      var old_user_items = spark.read.parquet(s"$output_dir/20200429")

      val left_сols = user_items.columns.toList
      val right_сols = old_user_items.columns.toList

      left_сols.diff(right_сols).foreach(x => user_items = user_items.withColumn(x, lit(null)))
      right_сols.diff(left_сols).foreach(x => old_user_items = old_user_items.withColumn(x, lit(null)))

      val all_cols = (left_сols ++ right_сols).sorted

      user_items = user_items.select(all_cols.map(col):_*)
                              .union(old_user_items.select(all_cols.map(col):_*))

      val sums = user_items.columns.map(x => sum(x).as(x))
      user_items = user_items.groupBy(col("uid")).agg(lit(1).as("for_arg"), sums.tail:_*)
                             .drop("for_arg")
    }

    user_items
      .write
      .parquet(s"$output_dir/$max_dt")

  }
}
