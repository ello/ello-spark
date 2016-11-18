package co.ello.impressions

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import com.amazonaws.auth.{BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.util.AwsHostNameUtils
import org.apache.log4j.{Level, Logger}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kinesis.KinesisUtils

import com.redislabs.provider.redis._

// Example at https://github.com/apache/spark/blob/master/external/kinesis-asl/src/main/scala/org/apache/spark/examples/streaming/KinesisWordCountASL.scala

object ElloStreamingCount {
  def main(args: Array[String]) {
    // Check that all required args were passed in.
    if (args.length != 6) {
      System.err.println(
        """
        | Usage: ElloStreamingImpressionCount <app-name> <stream-name> <endpoint-url> <batch-interval> <checkpoint-bucket-name> <redis-url>
        |
        |    <app-name> is the name of the consumer app, used to track the read data in DynamoDB
        |    <stream-name> is the name of the Kinesis stream
        |    <endpoint-url> is the endpoint of the Kinesis service
        |                   (e.g. https://kinesis.us-east-1.amazonaws.com)
        |    <batch-interval> is the number of milliseconds of data to roll into a Spark batch
        |    <checkpoint-bucket-name> is the name of an S3 bucket to store checkpoint data
        |    <redis-url> is the URL to a Redis host to store counts
        """.stripMargin)
      System.exit(1)
    }

    // Populate the appropriate variables from the given args
    val Array(appName, streamName, endpointUrl, interval, checkpointBucket, redisUrl) = args

    // Determine the number of shards from the stream using the low-level Kinesis Client from the AWS Java SDK.
    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
    require(credentials != null,
      "No AWS credentials found. Please specify credentials using one of the methods specified " +
      "in http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html")

    val kinesisClient = new AmazonKinesisClient(credentials)
    kinesisClient.setEndpoint(endpointUrl)
    val numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size

    // In this example, we're going to create 1 Kinesis Receiver/input DStream for each shard.
    // This is not a necessity; if there are less receivers/DStreams than the number of shards,
    // then the shards will be automatically distributed among the receivers and each receiver
    // will receive data from multiple shards.
    val numStreams = numShards

    // Spark Streaming batch interval
    val batchInterval = Milliseconds(interval.toLong * 1000)

    // Kinesis checkpoint interval is the interval at which the DynamoDB is updated with information
    // on sequence number of records that have been received. Same as batchInterval for this
    // example.
    val kinesisCheckpointInterval = batchInterval

    // Get the region name from the endpoint URL to save Kinesis Client Library metadata in
    // DynamoDB of the same region as the Kinesis stream
    val regionName = AwsHostNameUtils.parseRegionName(endpointUrl, "")

    // Set the path to checkpoint this application to/from
    val checkpointPath = s"s3n://$checkpointBucket/$appName"

    // Setup the SparkConfig and StreamingContext
    val ssc = StreamingContext.getActiveOrCreate(checkpointPath, () => {
      val sparkConfig = new SparkConf().setAppName("ElloStreamingImpressionCounts")

      // Shut down gracefully
      sparkConfig.set("spark.streaming.stopGracefullyOnShutdown","true")
      // sparkConfig.set("spark.streaming.receiver.writeAheadLog.enable", "true")
      sparkConfig.set("spark.streaming.driver.writeAheadLog.allowBatching", "false")

      val streamingContext = new StreamingContext(sparkConfig, batchInterval)

      // Log more verbosely
      streamingContext.sparkContext.setLogLevel("INFO")

      // Set up a checkpoint path
      streamingContext.checkpoint(checkpointPath)

      // Configure the Redis options
      val redisConfig = new RedisConfig(new RedisEndpoint(redisUrl))

      // Create the Kinesis DStreams
      val kinesisStreams = (0 until numStreams).map { i =>
        KinesisUtils.createStream(streamingContext, appName, streamName, endpointUrl, regionName,
          InitialPositionInStream.TRIM_HORIZON, kinesisCheckpointInterval, StorageLevel.MEMORY_AND_DISK_2)
      }

      // Union all the streams
      val unionStreams = streamingContext.union(kinesisStreams)

      // Convert each line of Array[Byte] to String, and split into words
      val impressions = unionStreams.flatMap(PostWasViewedDecoder(_))

      // Kick off the streaming aggregators
      val authorCheckpointPath = s"s3n://$checkpointBucket/authorSnapshots/"
      AggregateImpressionsByAuthor(redisConfig,
                                   authorCheckpointPath,
                                   impressions, FileSnapshotLoader(streamingContext.sparkContext, authorCheckpointPath))
      val postCheckpointPath = s"s3n://$checkpointBucket/postSnapshots/"
      AggregateImpressionsByPost(redisConfig,
                                 postCheckpointPath,
                                 impressions,
                                 FileSnapshotLoader(streamingContext.sparkContext, postCheckpointPath))
      streamingContext
    })

    // Start the streaming context and await termination
    ssc.start()
    ssc.awaitTermination()
  }
}
