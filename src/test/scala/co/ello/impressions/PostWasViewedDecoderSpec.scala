import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}
import java.nio.charset.StandardCharsets._
import java.nio.file.{Files, Paths}
import co.ello.impressions.PostWasViewedDecoder

class PostWasViewedDecoderSpec extends FunSpec with Matchers with BeforeAndAfter {

  describe("Decoding a valid Avro record with all fields") {
    it("returns a tuple of (post_id, author_id, viewer_id)") {
      val avroBytes = Files.readAllBytes(Paths.get("fixtures/post_was_viewed.avro"))
      PostWasViewedDecoder(avroBytes) shouldEqual Seq(("11", "1", "1"))
    }
  }

  describe("Decoding a valid Avro record with no viewer") {
    it("returns a tuple of (post_id, author_id, null)") {
      val avroBytes = Files.readAllBytes(Paths.get("fixtures/post_was_viewed_null_viewer.avro"))
      PostWasViewedDecoder(avroBytes) shouldEqual Seq(("11", "1", null))
    }
  }

  describe("Decoding a valid Avro record with the wrong event name") {
    it("returns an empty Seq") {
      val avroBytes = Files.readAllBytes(Paths.get("fixtures/post_was_loved.avro"))
      PostWasViewedDecoder(avroBytes) shouldEqual Seq()
    }
  }
}
