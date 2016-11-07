import co.ello.testing._
import com.redislabs.provider.redis._
import co.ello.impressions._
import java.nio.file.{Files, Paths}
import com.holdenkarau.spark.testing.{SharedSparkContext, RDDComparisons}
import org.apache.spark.sql.SQLContext

class RedisSnapshotLoaderSpec extends UnitSpec with Redis with SharedSparkContext with RDDComparisons {

  describe("Loading counts from Postgres and Redis") {
    it("returns an RDD of counts") {
      // Tee up Postgres with fixture data
      var postgresUrl = "postgres://localhost:5432/ello-spark"
      val postsJsonPath = Paths.get(getClass.getResource("/fixtures/posts.json").toURI).toString
      var sqlContext = new SQLContext(sc)
      var df = sqlContext.read.json(postsJsonPath)
      var dbProperties = new java.util.Properties
      df.write.mode("overwrite").options(Map("driver" -> "org.postgresql.Driver")).jdbc(JdbcUrlFromPostgresUrl(postgresUrl), "posts", dbProperties)

      // Tee up Redis with fixture data
      val countsTextPath = Paths.get(getClass.getResource("/fixtures/counts.txt").toURI).toString
      val countsRDD = sc.textFile(countsTextPath).map(_.split(",")).map { case Array(key, value) => (key, value) }
      sc.toRedisKV(countsRDD)

      // Run the loader and check
      val redisValues = RedisSnapshotLoader(sc, redisConfig, postgresUrl).collect()
      redisValues should contain (("1", "10", 10L))
      redisValues should contain (("2", "10", 0L))
      redisValues should contain (("3", "10", 0L))
      redisValues should contain (("4", "20", 0L))
      redisValues should contain (("5", "20", 0L))
      redisValues should contain (("6", "20", 0L))
    }
  }
}
