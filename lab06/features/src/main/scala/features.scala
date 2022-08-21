import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.ml.linalg.SparseVector

object features {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("patrakova_lab06")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val weblogs_path = "/labs/laba03/weblogs.json"
    val input_dir = "/user/ekaterina.patrakova//users-items/20200429"
    val output_dir = "/user/ekaterina.patrakova/features"

    val users_items = spark.read.parquet(input_dir)

    var logs = spark.read.json(weblogs_path)
      .select(col("uid"), explode(col("visits")).as("visits"))
      .select(col("uid"), col("visits")("url").as("domain"), col("visits")("timestamp").as("timestamp"))
      .withColumn("domain",  expr("regexp_replace(parse_url(domain, 'HOST'), '^www.', '')"))

    logs = logs.withColumn("utc", from_unixtime(col("timestamp")/1000))
      .withColumn("week_day", concat(lit("web_day_"), substring(lower(date_format(col("utc"), "EEEE")), 0, 3)))
      .withColumn("hour", hour(col("utc")))
      .withColumn("web_fraction", expr("""case
                                            when hour>=9 and hour<=17 then 'web_fraction_work_hours'
                                            when hour>=18 and hour<=23 then 'web_fraction_evening_hours'
                                        end"""))
      .withColumn("hour", concat(lit("web_hour_"), col("hour")))

    val week_days = logs.groupBy(col("uid"))
      .pivot("week_day")
      .count

    val hours = logs.groupBy(col("uid"))
      .pivot("hour")
      .count

    val web_fractions = logs.groupBy(col("uid"))
      .pivot("web_fraction")
      .count

    var result = logs.groupBy("uid").count
      .join(week_days, Seq("uid"))
      .join(hours, Seq("uid"))
      .join(web_fractions, Seq("uid"))
      .withColumn("web_fraction_evening_hours", col("web_fraction_evening_hours").divide(col("count")))
      .withColumn("web_fraction_work_hours", col("web_fraction_work_hours").divide(col("count")))
      .drop("null", "count").na.fill(0)
      .join(users_items, Seq("uid"), "left")


    val top_1000_domains = logs
      .filter(col("domain").isNotNull)
      .groupBy("domain").count
      .orderBy(col("count").desc)
      .limit(1000)

    val top_domains_agg = logs
      .join(broadcast(top_1000_domains), Seq("domain"), "inner")
      .groupBy(col("uid"))
      .agg(collect_list("domain").alias("domains"))

    val domains = top_1000_domains.select(col("domain")).as[String].collect.sorted
    val count_vectorizer: CountVectorizerModel = new CountVectorizerModel(domains)
      .setInputCol("domains")
      .setOutputCol("features")

    val getResVector = udf((v: SparseVector) => {
      val res = Array.fill(1000)(0)
      v.indices.indices.foreach { i => res(v.indices(i)) = v.values(i).toInt }
      res
    })

    val features = count_vectorizer
      .transform(top_domains_agg)
      .withColumn("domain_features", getResVector(col("features")))
      .drop("domains", "features")

    result
      .join(features, Seq("uid"), "left")
      .write
      .mode("append")
      .parquet(output_dir)



  }
}
