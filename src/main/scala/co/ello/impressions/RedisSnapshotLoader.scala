package co.ello.impressions

import org.apache.spark.rdd.RDD
import com.redislabs.provider.redis._
import com.redislabs.provider.redis.rdd.RedisKVRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext


object RedisSnapshotLoader {
  def apply(sparkContext: SparkContext, redisConfig: RedisConfig, postgresUrl: String): RDD[(String, String, Long)] = {
    // Pull initial RDD of post counts
    val opts = Map(
      "url" -> JdbcUrlFromPostgresUrl(postgresUrl),
      "driver" -> "org.postgresql.Driver",
      "numPartitions" -> "1000",
      "partitionColumn" -> "id",
      "lowerBound" -> "0",
      "upperBound" -> Int.MaxValue.toString,
      "dbtable" -> "posts"
    )
    val sqc = new SQLContext(sparkContext)
    val df = sqc.read.format("jdbc").options(opts).load
    val keys = df.select("id", "author_id").rdd.map { row =>
      ( s"posts:${row.getAs[Long]("id").toString}:impression_count",
        ( row.getAs[Long]("id").toString,
          row.getAs[Long]("author_id").toString ) )
    }

    // Fetch the Redis values at those keys.
    // This will pull these all back to the driver. Can't figure out a way around it ATM.
    val kv = sparkContext.fromRedisKV(keys.map(_._1).collect)
    val keysAndCounts = keys.leftOuterJoin(kv)
    keysAndCounts.map {
      case (key, ((id, author_id), count)) =>
        (id, author_id, count.getOrElse("0").toLong)
    }
  }
}
