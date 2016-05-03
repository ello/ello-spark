import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

object ElloRecommend {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Ello Recommend")
    val sc = new SparkContext(conf)


    // val dbUrl = "***REMOVED***";
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val prop = new java.util.Properties;
    val dbUri = new java.net.URI(args(0))

    val Array(username, password) = dbUri.getUserInfo().split(":")
    val dbUrl = "jdbc:postgresql://" + dbUri.getHost() + ':' + dbUri.getPort() + dbUri.getPath() + "?sslmode=require&user=" + username + "&password=" + password;
    val maxIdDF = sqlContext.read.jdbc(dbUrl, "(select max(id) as maxId from followerships) tmp", prop)
    maxIdDF.printSchema()

    val maxId = maxIdDF.select("maxid").collect()(0)(0).asInstanceOf[Number].longValue
    println("maxId = ", maxId)

    val jdbcDF = sqlContext.read.jdbc(dbUrl, "followerships", "id", 0.toLong, maxId, 250, prop)

    jdbcDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

    jdbcDF.printSchema()

    val ratings = jdbcDF.select("owner_id", "subject_id", "priority").filter("priority in ('friends', 'noise')").map( r =>
      Rating(r.getInt(0), r.getInt(1), 1.0)
    )

    // Build the recommendation model using ALS
    val rank = 50
    val numIterations = 10
    val model = ALS.trainImplicit(ratings, rank, numIterations)

    println("userFeatures = ", model.userFeatures.count)
    println("productFeatures = ", model.productFeatures.count)

    val topKRecs = model.recommendProducts(1, 10)
    println("topKRecs for user 1 = ", topKRecs.mkString("\n"))


    // Evaluate the model on rating data
    // val usersProducts = ratings.map { case Rating(user, product, rate) =>
    //   (user, product)
    // }
    // val predictions =
    //   model.predict(usersProducts).map { case Rating(user, product, rate) =>
    //     ((user, product), rate)
    //   }
    // val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
    //   ((user, product), rate)
    // }.join(predictions)
    // val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
    //   val err = (r1 - r2)
    //   err * err
    // }.mean()
    // println("Mean Squared Error = " + MSE)

    // val myFollowings = ratings.filter( f => f.owner_id === 1).select("subject_id")
    // val candidates = ratings.select("subject_id").filter(!myFollowings.contains(_))
    // val recommendations = model
    //   .predict(candidates.map((0, _)))
    //   .collect()
    //   .sortBy(- _.rating)
    //   .take(50)

    // var i = 1
    // println("Movies recommended for you:")
    // recommendations.foreach { r =>
    //   println("%2d".format(i) + ": " + r)
    //   i += 1
    // }

    // println(model.predict(1))

    // Shut it down
    sc.stop()
  }
}
