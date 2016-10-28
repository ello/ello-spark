import co.ello.testing._
import java.nio.charset.StandardCharsets._
import java.nio.file.{Files, Paths}
import co.ello.impressions._

class PostWasViewedDecoderSpec extends UnitSpec {

  class AvroFixture(path: String) {
    val avroBytes = Files.readAllBytes(Paths.get(getClass.getResource(s"/fixtures/$path.avro").toURI))
  }

  describe("Decoding a valid Avro record with all fields") {
    it("returns an Impression of (post_id, author_id, viewer_id)") {
      new AvroFixture("post_was_viewed") {
        PostWasViewedDecoder(avroBytes) shouldEqual Seq(Impression("11", "1", Some("1")))
      }
    }
  }

  describe("Decoding a valid Avro record with no viewer") {
    it("returns an Impression of (post_id, author_id, null)") {
      new AvroFixture("post_was_viewed_null_viewer") {
        PostWasViewedDecoder(avroBytes) shouldEqual Seq(Impression("11", "1", None))
      }
    }
  }

  describe("Decoding a valid Avro record with the wrong event name") {
    it("returns an empty Seq") {
      new AvroFixture("post_was_loved") {
        PostWasViewedDecoder(avroBytes) shouldEqual Seq()
      }
    }
  }
}
