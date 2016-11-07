package co.ello.impressions

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.MapWithStateDStream
import org.apache.spark.rdd.RDD
import com.redislabs.provider.redis._

object AggregateImpressionsToRedisByAuthor {
  def apply(redisConfig: RedisConfig, impressions: DStream[Impression]): Unit = {
    authorCountStreamFromImpressions(impressions).foreachRDD(saveAuthorCountsToRedis(redisConfig, _))
  }

  def authorCountStreamFromImpressions(impressions: DStream[Impression]): MapWithStateDStream[String, Long, Long, (String, Long)] = {
    // Map each impression to a (author_id, 1) tuple so we can reduce by key to count the impressions
    val authorCounts = impressions.map(i => (i.author_id, 1L)).reduceByKey(_ + _)

    // Set up the recurring state specs
    val authorStateSpec = StateSpec.function(CounterStateFunction.trackStateFunc _)

    // Incorporate this batch into the long-running state
    authorCounts.mapWithState(authorStateSpec)
  }

  def saveAuthorCountsToRedis(redisConfig: RedisConfig, rdd: RDD[(String, Long)]): Unit = {
    val kvRDD = rdd.map { case (id,count) => (s"author:$id:total_impression_count", count.toString) }
    kvRDD.foreachPartition(partition => RedisContext.setKVs(partition, 0, redisConfig))
  }
}
