package co.ello

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit


object ElloRecommend {

  def maxIdFromTable(spark: SparkSession, dbUri: String, tableName: String): Long = {
    val opts = Map(
      "url" -> dbUri,
      "driver" -> "org.postgresql.Driver",
      "dbtable" -> s"(select max(id) as maxId from ${tableName}) tmp"
    )
    val maxIdDF = spark.read.format("jdbc").options(opts).load()
    return maxIdDF.select("maxid").collect()(0)(0).asInstanceOf[Number].longValue
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Ello Follower Recommendations")
      .getOrCreate()

    spark.sparkContext.setCheckpointDir("tmp/checkpoint")

    val dbUri = JdbcUrlFromPostgresUrl(args(0))
    val tableName = "followerships"

    val maxId = maxIdFromTable(spark, dbUri, tableName)
    println(s"maxId = $maxId")

    val opts = Map(
      "url" -> dbUri,
      "driver" -> "org.postgresql.Driver",
      "numPartitions" -> "1000",
      "partitionColumn" -> "id",
      "lowerBound" -> "0",
      "upperBound" -> maxId.toString,
      "dbtable" -> tableName
    )

    val df = spark.read.format("jdbc").options(opts).load

    val ratings = df.select("owner_id", "subject_id", "priority")
      .filter("priority in ('friends', 'noise')")
      .withColumn("rating", lit(1.0))
      .cache()

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(10)
      .setImplicitPrefs(true)
      .setUserCol("owner_id")
      .setItemCol("subject_id")
      .setRatingCol("rating")

    val paramGrid = new ParamGridBuilder()
      .addGrid(als.rank, Array(5, 10, 20, 40))
      .addGrid(als.regParam, Array(0.1, 1.0, 10.0))
      .build()

    val rmseEval = new RegressionEvaluator()
        .setMetricName("rmse")
        .setLabelCol("rating")
        .setPredictionCol("prediction")

    // Fit and evaluate against the entire dataset -
    // split/cross validation with a random split won't work with ALS
    // because users outside the training set won't have results
    // This is largely identical to the implementation of TrainValidationSplit
    val numModels = paramGrid.length
    val metrics = new Array[Double](paramGrid.length)
    val models = als.fit(ratings, paramGrid)

    var i = 0
    for (i <- 0 until numModels) {
      val metric = rmseEval.evaluate(models(i).transform(ratings, paramGrid(i)))
      println(s"Got metric $metric for model trained with ${paramGrid(i)}.")
      metrics(i) += metric
    }

    println(s"Train validation split metrics: ${metrics.toSeq}")

    val (bestMetric, bestIndex) =
      if (rmseEval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
      else metrics.zipWithIndex.minBy(_._1)
    println(s"Best set of parameters:\n${paramGrid(bestIndex)}")
    println(s"Best train validation split metric: $bestMetric.")

    val bestModel = models(bestIndex)
    bestModel.write.overwrite().save("tmp/model")

    spark.stop()
  }
}
