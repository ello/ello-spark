package co.ello.impressions

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object FileSnapshotLoader {
  def apply(sparkContext: SparkContext, pathPrefix: String): RDD[(String, Long)] = {
    splitStringRDD(sparkContext.textFile(s"$pathPrefix*"))
  }

  def splitStringRDD(stringRDD: RDD[String]): RDD[(String, Long)] = {
    stringRDD.map(_.split(",")).map { case Array(id, count) => (id, count.toLong) }
  }
}
