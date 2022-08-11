import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object users_items {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("patrakova_lab05")
      .getOrCreate

    val input_dir = spark.conf.get("spark.users_items.input_dir")
    val output_dir = spark.conf.get("spark.users_items.output_dir")
    val update = spark.conf.get("spark.users_items.update")

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

    var user_items = views_pivot.join(buy_pivot, Seq("uid"), "outer")

    if (update == "1") {
      var old_user_items = spark.read.parquet(s"$output_dir/20200429")

      user_items = user_items.union(old_user_items)

      val sums = user_items.columns.map(x => sum(x).as(x))
      user_items = user_items.groupBy(col("uid")).agg(lit(0), sums.tail:_*)
    }

    val user_items_final = user_items.na.fill(0, user_items.columns)
    user_items_final
      .repartition(200)
      .write
      .mode("append")
      .parquet(s"$output_dir/$max_dt")

  }
}
