import co.ello.testing._
import com.redislabs.provider.redis._
import com.holdenkarau.spark.testing.StreamingActionBase
import org.apache.spark.streaming.dstream.DStream
import co.ello.impressions._

class AggregateImpressionsToRedisByPostSpec extends UnitSpec with StreamingActionBase with Redis {
  describe("Aggregating impression counts by post") {
    it("stores the current counts in Redis") {
      var batch1 = List(Impression("1", "1", Some("1")),
        Impression("2", "1", Some("1")),
        Impression("3", "2", Some("1")))
      var batch2 = List(Impression("1", "1", Some("1")),
        Impression("2", "1", Some("1")),
        Impression("3", "2", Some("1")))
      var batches = List(batch1, batch2)
      runAction(batches, (b: DStream[Impression]) => AggregateImpressionsToRedisByPost(redisConfig, b))

      val redisValues = sc.fromRedisKV("post:*:impression_count").collectAsMap()
      redisValues should contain ("post:1:impression_count" -> "2")
      redisValues should contain ("post:2:impression_count" -> "2")
      redisValues should contain ("post:3:impression_count" -> "2")
    }
  }
}
