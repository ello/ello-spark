import co.ello.testing._
import com.redislabs.provider.redis._
import com.holdenkarau.spark.testing.StreamingActionBase
import org.apache.spark.streaming.dstream.DStream
import co.ello.impressions._

class AggregateImpressionsByPostSpec extends UnitSpec with StreamingActionBase with Redis {
  before {
    var batch1 = List(Impression("1", "1", Some("1")),
      Impression("2", "1", Some("1")),
      Impression("3", "2", Some("1")))
    var batch2 = List(Impression("1", "1", Some("1")),
      Impression("2", "1", Some("1")),
      Impression("3", "2", Some("1")))
    var batches = List(batch1, batch2)
    var initialState = sc.emptyRDD[(String,Long)]
    runAction(batches, (b: DStream[Impression]) => AggregateImpressionsByPost(redisConfig, "tmp/snapshots/post/", b, initialState))
  }

  describe("Aggregating impression counts by post") {
    it("stores the current counts in Redis") {
      val redisValues = sc.fromRedisKV("post:*:impression_count").collectAsMap()
      redisValues should contain ("post:1:impression_count" -> "2")
      redisValues should contain ("post:2:impression_count" -> "2")
      redisValues should contain ("post:3:impression_count" -> "2")
    }

    it("stores a snapshot of the current counts as flat files") {
      val textValues = sc.textFile("./tmp/snapshots/post/*").collect()
      textValues should contain ("1,2")
      textValues should contain ("2,2")
      textValues should contain ("3,2")
    }
  }
}
