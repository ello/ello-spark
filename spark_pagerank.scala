import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.VertexId;
import org.apache.spark.rdd.RDD;

// Generated via `heroku pg:psql ROSE -a ello-production -c "\copy (select owner_id, subject_id, priority from followerships) to 'relationships.csv' with csv;"`
val relationshipsFile = sc.textFile("relationships.csv").cache();
val edgesRDD: RDD[(VertexId, VertexId)] = relationshipsFile.map(line => line.split(",")).map(line => (line(0).toLong, line(1).toLong));
val graph = Graph.fromEdgeTuples(edgesRDD, 1);
val ranks = graph.pageRank(0.0001).vertices;

// Generated via `heroku pg:psql ROSE -a ello-production -c "\copy (select id, username from users) to 'users.csv' with csv;"`
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
