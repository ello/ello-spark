package co.ello.impressions

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.MapWithStateDStream
import org.apache.spark.rdd.RDD
import com.redislabs.provider.redis._

object AggregateImpressionsByPost {
  def apply(redisConfig: RedisConfig, filePath: String, impressions: DStream[Impression], initialState: RDD[(String, Long)]): Unit = {
    var dstream = postCountStreamFromImpressions(impressions, initialState)
    dstream.foreachRDD(savePostCountsToRedis(redisConfig, _))
    dstream.stateSnapshots.foreachRDD { rdd =>
      rdd.map { case (post, count)  => s"$post,$count" }.saveAsTextFile(filePath)
    }
  }

  def postCountStreamFromImpressions(impressions: DStream[Impression], initialState: RDD[(String, Long)]): MapWithStateDStream[String, Long, Long, (String, Long)] = {
    // Map each impression to a (post_id, 1) tuple so we can reduce by key to count the impressions
    val postCounts = impressions.map(i => (i.post_id, 1L)).reduceByKey(_ + _)

    // Set up the recurring state specs
    val postStateSpec = StateSpec.function(CounterStateFunction.trackStateFunc _).initialState(initialState)

    // Incorporate this batch into the long-running state
    postCounts.mapWithState(postStateSpec)
  }

  def savePostCountsToRedis(redisConfig: RedisConfig, rdd: RDD[(String, Long)]): Unit = {
    val kvRDD = rdd.map { case (id,count) => (s"post:$id:impression_count", count.toString) }
    kvRDD.foreachPartition(partition => RedisContext.setKVs(partition, 0, redisConfig))
  }
}
