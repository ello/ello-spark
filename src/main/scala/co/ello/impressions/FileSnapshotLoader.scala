package co.ello.impressions

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger

object FileSnapshotLoader {
  def apply(sparkContext: SparkContext, pathPrefix: String): RDD[(String, Long)] = {
    val path = new Path(pathPrefix);
    val pathPrefixWithWildcard = s"$pathPrefix*"
    if(path.getFileSystem(sparkContext.hadoopConfiguration).globStatus(new Path(pathPrefixWithWildcard)).isEmpty) {
      Logger.getLogger(getClass.getName).info(s"No snapshot found at $pathPrefix")
      sparkContext.emptyRDD[(String, Long)]
    } else {
      val rdd = splitStringRDD(sparkContext.textFile(pathPrefixWithWildcard))
      Logger.getLogger(getClass.getName).info(s"Loaded snapshot of ${rdd.count} impression counts from $pathPrefix")
      rdd
    }
  }

  def splitStringRDD(stringRDD: RDD[String]): RDD[(String, Long)] = {
    stringRDD.map(_.split(",")).map { case Array(id, count) => (id, count.toLong) }
  }
}
