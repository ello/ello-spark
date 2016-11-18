import co.ello.testing._
import com.redislabs.provider.redis._
import com.holdenkarau.spark.testing.StreamingActionBase
import org.apache.spark.streaming.dstream.DStream
import co.ello.impressions._

class AggregateImpressionsByAuthorSpec extends UnitSpec with StreamingActionBase with Redis {
  before {
    var batch1 = List(Impression("1", "1", Some("1")),
      Impression("2", "1", Some("1")),
      Impression("3", "2", Some("1")))
    var batch2 = List(Impression("1", "1", Some("1")),
      Impression("2", "1", Some("1")),
      Impression("3", "2", Some("1")))
    var batches = List(batch1, batch2)
    var initialState = sc.emptyRDD[(String, Long)]
    runAction(batches, (b: DStream[Impression]) => AggregateImpressionsByAuthor(redisConfig, "tmp/snapshots/author/", b, initialState))
  }

  describe("Aggregating impression counts by author") {
    it("stores the current counts in Redis") {
      val redisValues = sc.fromRedisKV("author:*:total_impression_count").collectAsMap()
      redisValues should contain ("author:1:total_impression_count" -> "4")
      redisValues should contain ("author:2:total_impression_count" -> "2")
    }

    it("stores a snapshot of the current counts as flat files") {
      val textValues = sc.textFile("./tmp/snapshots/author/*").collect()
      textValues should contain ("1,4")
      textValues should contain ("2,2")
    }
  }
}
