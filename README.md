# ello-spark
Noodling with Apache Spark for machine learning

## Background
I was initially resistant to exploring Spark in depth. For one, Scala frightened me (it still does, but the barrier to entry is less crazy than I thought it was). I also didn't think our data was big enough to need a hammer like Spark. After spending a few cycles monkeying around with both, I'm a convert. I've barely scratched the surface of what can be done, but I'm definitely convinced of the power of this toolset.

A bit more background:

- I originally tried doing PageRank with [neo4j-mazerunner](https://github.com/neo4j-contrib/neo4j-mazerunner), which is a pretty neat tool that lets you export a graph from Neo4J into Spark for processing, then return it back to Neo4J for querying. It's super fancy, and I can see a user case for it, but I could never get it fully working locally. I think I was trying to throw way too much data at at: importing into Neo was a pain in the ass, everything was always running out of memory, I was running it in Docker and that was hard to manage, etc. I think there's too much machinery involved for running locally on any reasonably large dataset.

- I also looked a lot at the Clojure ecosystem and how it interacts with Spark.
  Clojure is a fascinating language to me personally, and it felt a bit less
  daunting to dig into than Scala (at least, from what I've heard). There are
  [some great](https://speakerdeck.com/chris_betz/spark-way)
  [presentations](https://speakerdeck.com/chris_betz/big-data-processing-using-apache-spark-and-clojure) on using Spark and Clojure together, and at least two libraries for accomplishing the same ([sparkling](https://github.com/gorillalabs/sparkling) and [flambo](https://github.com/yieldbot/flambo)), but neither supports the full range of Spark goodies that I wanted to explore - GraphX, ML/Mllib, and DataFrames. I felt like going this route would still take a lot of learning and ultimately wind up still feeling like a second-class citizen in the Spark world because of limitations in the libraries (not that the libraries themselves aren't awesome though).

- I spent a little bit of time with Pyspark. Python feels like fairly
  familiar territory coming from Ruby, but I'm definitely still not a Python
  programmer. The bigger issue is that Pyspark still lacks support for much of GraphX and Mllib.

- There is a [JRuby/Spark bridge](https://github.com/ondra-m/ruby-spark) but it's pretty rudimental.

- Amazon's EMR service [supports Spark too](https://aws.amazon.com/elasticmapreduce/details/spark/), which makes the operational side of things much easier. EMR even has a nice feature where you can spin up a cluster just for the purpose of running a single job, then have it automatically terminate at the end of the run so you're not paying for idle resources. And, as of just a few weeks ago, you can even [set it up via Cloudformation](https://aws.amazon.com/about-aws/whats-new/2016/02/aws-cloudformation-adds-support-for-amazon-vpc-nat-gateway-amazon-ec2-container-registry-and-more/).

Other things to investigate

- My current experimenting involves pulling CSVs out of Postgres and reading
  them into Spark manually. It'd be nice to figure out how to pull from Postgres
  directly with a JdbcRDD or a SchemaRDD. Or even Kinesis directly!

## Usage

First, install Scala, Spark and SBT via homebrew:

    $ brew install apache-spark scala sbt

From there, you can poke around in the Spark Shell by running:

    $ spark-shell --driver-cores 2 --driver-memory 8G

You can also build/run the [PageRank example](src/main/scala/ElloPageRank.scala):

    $ heroku pg:psql ROSE -a ello-production -c "\copy (select owner_id, subject_id, priority from followerships) to 'relationships.csv' with csv;"
    $ heroku pg:psql ROSE -a ello-production -c "\copy (select id, username from users) to 'users.csv' with csv;"
    $ sbt package
    $ spark-submit --class "ElloPageRank" --master "local[*]" --driver-cores 2 --driver-memory 8G target/scala-2.10/ello-pagerank_2.10-1.0.jar

While jobs are executing (in the shell or in batch), you can visit `http://localhost:4040` to check their status.
