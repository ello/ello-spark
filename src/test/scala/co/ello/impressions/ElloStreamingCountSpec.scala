import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}
import com.holdenkarau.spark.testing.StreamingSuiteBase
import co.ello.impressions._

class ElloStreamingCountSpec extends FunSpec with Matchers with BeforeAndAfter with StreamingSuiteBase {


  describe("Computing counts by post") {
    it("does the right thing") {
      val batch1 = List(Impression("1", "1", "1"), Impression("2", "1", "1"), Impression("3", "2", "1"))
      val batch2 = List(Impression("1", "1", "1"), Impression("2", "1", "1"), Impression("3", "2", "1"))
      val batches = List(batch1, batch2)
      val expected = List(List(("1", 2L), ("2", 1L)), List(("1", 4L), ("2", 2L)))
      testOperation[Impression, Tuple2[String, Long]](batches, ElloStreamingCount.authorCountStreamFromImpressions _, expected, ordered = true)
    }
  }
}
