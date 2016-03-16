import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.VertexId;
import org.apache.spark.rdd.RDD;

object ElloPageRank {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Ello PageRank")
    val sc = new SparkContext(conf)

    val relationshipsFile = sc.textFile("relationships.csv").cache();
    val edgesRDD: RDD[(VertexId, VertexId)] = relationshipsFile.map(line => line.split(",")).map(line => (line(0).toLong, line(1).toLong));
    val graph = Graph.fromEdgeTuples(edgesRDD, 1);
    val ranks = graph.pageRank(0.0001).vertices;

    val usersFile = sc.textFile("users.csv");
    val users = usersFile.map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }

    val sortedRanks = ranksByUsername.sortBy(_._2, false)
    println(sortedRanks.take(250).mkString("\n"))

    sc.stop()
  }
}
