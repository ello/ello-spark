package co.ello.recommend

import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

object RecommendFollows {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Ello Make Follower Recommendations")
      .getOrCreate()

    spark.sparkContext.setCheckpointDir("tmp/checkpoint")

    // Build the recommendation model using ALS on the training data
    val model = MatrixFactorizationModel.load(spark.sparkContext, "var/mfmodel")
    model.productFeatures.cache()
    model.userFeatures.cache()
    val recs = model.recommendProductsForUsers(10)

    recs.take(10).foreach(println)

    spark.stop()
  }
}
