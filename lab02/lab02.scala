import java.net.URLDecoder
import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.functions._
import scala.math.pow
import org.apache.spark.sql.SparkSession

object filter {
  val decode = udf { (url: String) =>
    Try(URLDecoder.decode(url, "UTF-8")) match {
      case Success(url: String) => url
      case Failure(exc) => ""
    }
  }

  val get_domain = udf { (str: String) => {
    str.split("/")(0)
    }
  }

  val pow_df = udf { (x: Double, y: Double) =>
    pow(x, y)
  }

    val spark = SparkSession
      .builder()
      .appName("sql")
      .getOrCreate()

    var autousers = spark.read.json("/labs/laba02/autousers.json")
      .toDF()
      .select(explode(col("autousers")).as("UID"), lit(1).as("autousers"))

    var df = spark.read.option("header", "false")
      .option("encoding", "utf8")
      .option("delimiter", "\t")
      .csv("/labs/laba02/logs")
      .withColumnRenamed("_c0", "UID")
      .withColumnRenamed("_c1", "timestamp")
      .withColumnRenamed("_c2", "URL")

    df = df.join(autousers, Seq("UID"), "left")
    df = df.na.fill(0, Seq("autousers"))
    df = df.na.fill("")
    df = df.withColumn("URL_preprocessed", regexp_replace(get_domain(regexp_replace(decode(col("URL")),
      "^(https://)|(http://)", "")), "^(www.)", ""))

    df = df.filter("URL like 'http%' or URL like 'https%'")

    var auto = df.filter(col("autousers") === 1).count()
    var all_sites = df.count().toFloat
    var url_auto = df.filter(col("autousers") === 1)
      .groupBy(col("URL_preprocessed")).sum()
      .withColumn("auto_site_prob", col("sum(autousers)").divide(all_sites))
    var url = df.groupBy(col("URL_preprocessed")).count().as("cnt")
      .withColumn("site_prob", col("count").divide(all_sites))
    var result = url.join(url_auto, Seq("URL_preprocessed"), "left").na.fill(0)
    var auto_user_prob = auto.toFloat / all_sites

    result = result.withColumn("result",
      (pow_df(col("auto_site_prob"), lit(2)) / (col("site_prob") * lit(auto_user_prob))).cast(DecimalType(26, 15)))
      .select("URL_preprocessed", "result")
      .orderBy(col("result").desc, col("URL_preprocessed").asc)

    val result_200 = result.take(200)
    val result_200_txt = result_200.mkString("\n")
      .replace(",", "\t")
      .replace("[", "")
      .replace("]", "")

    reflect.io.File("../laba02_domains.txt").writeAll(result_200_txt)

}