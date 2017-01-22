package co.ello.recommend

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions.lit
import co.ello.JdbcUrlFromPostgresUrl
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating


object RecommendModelBuilder {

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
      .appName("Ello Build Recommendation Model")
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

    val sourceDF = spark.read.format("jdbc").options(opts).load

    val ratingsDF = sourceDF.select("owner_id", "subject_id", "priority")
      .filter("priority in ('friends', 'noise')")
      .withColumn("rating", lit(1.0))
      .cache()

    val ratingsRdd = ratingsDF.rdd.map {
      case Row(owner_id: Int, subject_id: Int, priority: String, rating: Double) =>
        Rating(owner_id, subject_id, rating)
    }

    // Build the recommendation model using ALS on the training data
    val rank = args(1).toInt
    val iterations = args(2).toInt
    val reg = args(3).toDouble
    val model = ALS.trainImplicit(ratingsRdd, rank, iterations, reg, 1.0)
    model.save(spark.sparkContext, "var/mfmodel")

    spark.stop()
  }
}
