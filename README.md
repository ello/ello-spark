<img src="http://d324imu86q1bqn.cloudfront.net/uploads/user/avatar/641/large_Ello.1000x1000.png" width="200px" height="200px" />

# Ello Spark Jobs

[![Build Status](https://travis-ci.org/ello/spark-jobs.svg?branch=master)](https://travis-ci.org/ello/spark-jobs)

Spark jobs written in Scala to support a variety of data-crunching activities for [ello.co](http://ello.co).

## Background
I was initially resistant to exploring Spark in depth. For one, Scala frightened me. I also didn't think our data was big enough to need a hammer like Spark. After spending a few cycles monkeying around with both, I'm a convert. Scala still scares me, but the barrier to entry is less crazy than I initially thought. I've barely scratched the surface of what can be done, but I'm definitely sold on the power of this toolset.

Here's how I got there:

- I originally tried computing PageRank with [neo4j-mazerunner](https://github.com/neo4j-contrib/neo4j-mazerunner), which is a pretty neat tool that lets you export a graph from Neo4J into Spark for processing, then return it back to Neo4J for querying. It's super fancy, and I can see a user case for it, but I could never get it fully working locally. I think I was trying to throw way too much data at it: importing into Neo was a pain in the ass, everything was always running out of memory, I was running it in Docker and that was hard to manage, etc. I think there's too much machinery involved for running locally on any reasonably large dataset.

- I also looked at the Clojure ecosystem and how it interacts with Spark. Clojure is a fascinating language to me personally, and it felt a bit less daunting to dig into than Scala. There are [some great](https://speakerdeck.com/chris_betz/spark-way) [presentations](https://speakerdeck.com/chris_betz/big-data-processing-using-apache-spark-and-clojure) on using Spark and Clojure together, and at least two libraries for it ([sparkling](https://github.com/gorillalabs/sparkling) and [flambo](https://github.com/yieldbot/flambo)), but neither supports the full range of Spark goodies that I wanted to explore - GraphX, ML/Mllib, and DataFrames. I felt like going this route would still take a lot of learning and ultimately not let me explore what I wanted to because of limitations in the libraries.

- I spent a little bit of time with Pyspark as well. Python feels like fairly familiar territory coming from Ruby, but I'm definitely not a Python guy. The larger issue, however, is that Pyspark still lacks support for much of GraphX and MLlib, which meant many of the same drawbacks as Clojure above.

- There is a [JRuby/Spark bridge](https://github.com/ondra-m/ruby-spark) too but it's pretty rudimentary.

- [EclairJS](https://github.com/EclairJS/eclairjs-node) lets you use Spark from Node, but it's still in somewhat early development and has a lot of moving parts. At this point, I was starting to feel like I was going a long way around attempting to avoid learning Scala, and I might have saved myself some time by just sucking it up and learning the language from the get-go.

- On the deployment side, Amazon's EMR service [supports Spark natively](https://aws.amazon.com/elasticmapreduce/details/spark/), which makes the operational side of things much easier. EMR even has a nice feature where you can spin up a cluster just for the purpose of running a single job, then have it automatically terminate at the end of the run so you're not paying for idle resources. And, as of just a few weeks ago, you can even [set it up via Cloudformation](https://aws.amazon.com/about-aws/whats-new/2016/02/aws-cloudformation-adds-support-for-amazon-vpc-nat-gateway-amazon-ec2-container-registry-and-more/).

Other things to investigate

- Redis could be a great intermediate store for serving computed results to end users: https://github.com/RedisLabs/spark-redis
- https://docs.google.com/presentation/d/1YMPJGo62hRPDz1iXQK2fITJAFOJn6H3oPD5vsrMwA1c/edit?pref=2&pli=1#slide=id.g59ff6204c_0_116
- https://gist.github.com/erikerlandson/2f2b3dfe03f3266b577c
- https://github.com/holdenk/learning-spark-examples/blob/master/src/main/java/com/oreilly/learningsparkexamples/java/MLlib.java
- https://www.packtpub.com/books/content/building-recommendation-engine-spark

## Usage

First, install Scala, Spark and SBT via homebrew:

    $ brew install apache-spark scala sbt

From there, you can poke around in the Spark Shell by running:

    $ spark-shell --driver-cores 2 --driver-memory 8G

I have two examples in here so far:


### User Relationship PageRank (Eigenvector Centrality)

The [PageRank example](src/main/scala/ElloPageRank.scala) runs PageRank on all of the relationships in the network and outputs the users with the highest PageRank. It does not currently connect to Postgres to pull its data, so you'll need to do a bit of prep to run it.

First, retrieve the relationship and user data in CSV form from a prod replica:

    $ heroku pg:psql DATABASE -c "\copy (select owner_id, subject_id, priority from followerships) to 'relationships.csv' with csv;"
    $ heroku pg:psql DATABASE -c "\copy (select id, username from users) to 'users.csv' with csv;"

Second, build the Scala jar:

    $ sbt package

Finally, run the job via `spark-submit`:

    $ spark-submit --class "ElloPageRank" --master "local[*]" --driver-cores 2 --driver-memory 8G target/scala-2.10/ello-spark.10-1.0.jar

While jobs are executing (in the shell or in batch), you can visit `http://localhost:4040` to check their status. When it completes, it will output the top users and their PageRank to the console.


### Follower Recommendation (Alternating Least Squares)

The [Recommended Followers example](src/main/scala/ElloRecommend.scala) runs the ALS algorithm on all of the relationships in the network and outputs a set of recommended followers for that user. It connects to Postgres using a JDBC DataFrame to pull its data, which is slightly slower, but does not require any prep data. You will need to pull a `DATABASE_URL` for a prod replica to use it, however.

First, build the Scala jar as a full assembly (this gets rid of the need to mess
with your classpath):

    $ sbt assembly

Then, locate and copy the Postgres connection URL:

    $ heroku pg:credentials -a ello-production

Finally, run the job via `spark-submit`:

    $ spark-submit --class "ElloRecommend" --master "local[*]" --driver-cores 2 --driver-memory 8G target/scala-2.10/ello-spark_2.10-1.0.jar <DATABASE_URL>

While jobs are executing (in the shell or in batch), you can visit `http://localhost:4040` to check their status. When it completes, it will output the recommended users and their rating to the console.


### Streaming Post Impression Counts

The [Streaming Count example](src/main/scala/co/ello/impressions/ElloStreamingCount.scala) runs a Kinesis Client Library consumer to subscribe to incoming Kinesis events that indicate post impressions, and sums them as it goes, on both post and author dimensions.

It requires that your AWS credentials be set as environment variables.

First, build the Scala jar as a full assembly (this gets rid of the need to mess with your classpath):

    $ sbt assembly

Then, run the job via `spark-submit`:

    $ spark-submit --class "co.ello.impressions.ElloStreamingCount" --master "local[*]" --driver-cores 2 --driver-memory 8G target/scala-2.10/ello-Spark-assembly-1.0.jar <KCL application name> <kinesis stream name> <kinesis endpoint> <spark batch interval> <KCL checkpoint interval> <S3 bucket for checkpoints> <Redis URL for storing counts>

While jobs are executing (in the shell or in batch), you can visit `http://localhost:4040` to check their status. As it runs, it will output the top viewed posts to the console.

## Code of Conduct
Ello was created by idealists who believe that the essential nature of all human beings is to be kind, considerate, helpful, intelligent, responsible, and respectful of others. To that end, we will be enforcing [the Ello rules](https://ello.co/wtf/policies/rules/) within all of our open source projects. If you don’t follow the rules, you risk being ignored, banned, or reported for abuse.

## Contributing
Bug reports and pull requests are welcome on GitHub at https://github.com/ello/spark-jobs.

## License
Released under the [MIT License](/LICENSE.txt)
