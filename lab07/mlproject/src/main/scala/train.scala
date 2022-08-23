import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
import org.apache.spark.ml.{Pipeline}

object train {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("patrakova_lab07")
      .getOrCreate

    val train_path = spark.conf.get("spark.mlproject.train_path")
    val model_path = spark.conf.get("spark.mlproject.model_path")
    val max_iter = spark.conf.get("spark.mlproject.max_iter")
    val reg = spark.conf.get("spark.mlproject.reg_param")

    val logs = spark
      .read
      .json(train_path)
      .select(col("uid"), col("gender_age"), explode(col("visits")).as("visits"))
      .withColumn("domain",  expr("regexp_replace(parse_url(visits.url, 'HOST'), '^www.', '')"))
      .groupBy(col("uid"), col("gender_age"))
      .agg(collect_list("domain").alias("domains"))

    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")
      .fit(logs)

    val lr = new LogisticRegression()
      .setMaxIter(max_iter.toInt)
      .setRegParam(reg.toDouble)

    val converter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("label_string")
      .setLabels(indexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(cv, indexer, lr, converter))

    val model = pipeline.fit(logs)
    model.write.overwrite.save(model_path)
  }
}
