package co.ello.impressions

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD
import com.redislabs.provider.redis._

object AggregateImpressionsToRedisByPost {
  def apply(redisConfig: RedisConfig, impressions: DStream[Impression]): Unit = {
    postCountStreamFromImpressions(impressions).foreachRDD(savePostCountsToRedis(redisConfig, _))
  }

  def postCountStreamFromImpressions(impressions: DStream[Impression]) = {
    // Map each impression to a (post_id, 1) tuple so we can reduce by key to count the impressions
    val postCounts = impressions.map(i => (i.post_id, 1)).reduceByKey(_ + _)

    // Set up the recurring state specs
    val postStateSpec = StateSpec.function(CounterStateFunction.trackStateFunc _)

    // Incorporate this batch into the long-running state
    postCounts.mapWithState(postStateSpec)
  }

  def savePostCountsToRedis(redisConfig: RedisConfig, rdd: RDD[(String, Long)]) = {
    val kvRDD = rdd.map { case (id,count) => (s"post:$id:impression_count", count.toString) }
    kvRDD.foreachPartition(partition => RedisContext.setKVs(partition, 0, redisConfig))
  }
}
