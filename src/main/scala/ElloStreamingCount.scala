import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.nio.ByteBuffer

import com.amazonaws.auth.{BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.util.AwsHostNameUtils
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.PutRecordRequest
import org.apache.log4j.{Level, Logger}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.apache.spark.streaming._

import org.apache.avro.file.{DataFileReader, SeekableByteArrayInput}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

import scala.collection.JavaConversions._

// Example at https://github.com/apache/spark/blob/master/external/kinesis-asl/src/main/scala/org/apache/spark/examples/streaming/KinesisWordCountASL.scala

object ElloStreamingImpressionCount {
  def main(args: Array[String]) {
    // Check that all required args were passed in.
    if (args.length != 3) {
      System.err.println(
        """
        |Usage: ElloStreamingImpressionCount <app-name> <stream-name> <endpoint-url> <region-name>
        |
        |    <app-name> is the name of the consumer app, used to track the read data in DynamoDB
        |    <stream-name> is the name of the Kinesis stream
        |    <endpoint-url> is the endpoint of the Kinesis service
        |                   (e.g. https://kinesis.us-east-1.amazonaws.com)
          |
          |Generate input data for Kinesis stream using the example KinesisWordProducerASL.
          |See http://spark.apache.org/docs/latest/streaming-kinesis-integration.html for more
          |details.
          """.stripMargin)
      System.exit(1)
    }


    // Populate the appropriate variables from the given args
    val Array(appName, streamName, endpointUrl) = args


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
    val batchInterval = Milliseconds(2000)

    // Kinesis checkpoint interval is the interval at which the DynamoDB is updated with information
    // on sequence number of records that have been received. Same as batchInterval for this
    // example.
    val kinesisCheckpointInterval = batchInterval

    // Get the region name from the endpoint URL to save Kinesis Client Library metadata in
    // DynamoDB of the same region as the Kinesis stream
    val regionName = AwsHostNameUtils.parseRegionName(endpointUrl, "")

    // Set the path to checkpoint this application to/from
    val checkpointPath = s"s3n://ello-spark-checkpoints/$appName"

    // Setup the SparkConfig and StreamingContext
    val ssc = StreamingContext.getActiveOrCreate(checkpointPath, () => {
      val sparkConfig = new SparkConf().setAppName("ElloStreamingImpressionCounts")

      // Shut down gracefully
      sparkConfig.set("spark.streaming.stopGracefullyOnShutdown","true")

      val streamingContext = new StreamingContext(sparkConfig, batchInterval)

      // Set up a checkpoint path
      streamingContext.checkpoint(checkpointPath)


      // Create the Kinesis DStreams
      val kinesisStreams = (0 until numStreams).map { i =>
        KinesisUtils.createStream(streamingContext, appName, streamName, endpointUrl, regionName,
          InitialPositionInStream.TRIM_HORIZON, kinesisCheckpointInterval, StorageLevel.MEMORY_AND_DISK_2)
      }

      // Set up the recurring state specs
      val postStateSpec = StateSpec.function(trackStateFunc _)
      val authorStateSpec = StateSpec.function(trackStateFunc _)

      // Union all the streams
      val unionStreams = streamingContext.union(kinesisStreams)

      // Convert each line of Array[Byte] to String, and split into words
      val impressions = unionStreams.flatMap { byteArray =>
        val datumReader = new GenericDatumReader[GenericRecord]()
        val seekableInput = new SeekableByteArrayInput(byteArray)
        val dataFileReader = new DataFileReader[GenericRecord](seekableInput, datumReader)

        object PostWasViewedRecord {
          def unapply(record : GenericRecord): Option[GenericRecord] =
            if (record.getSchema().getName() == "post_was_viewed") Some(record) else None
        }

        dataFileReader.iterator().toSeq collect { case PostWasViewedRecord(record) => (record.get("post").asInstanceOf[GenericRecord].get("id").toString(),
                                                                                        record.get("author").asInstanceOf[GenericRecord].get("id").toString(),
                                                                                        Option(record.get("viewer").asInstanceOf[GenericRecord]) match {
                                                                                          case Some(viewer) => viewer.get("id").toString()
                                                                                          case None => null
        }) }

        // val sb = Seq.newBuilder[S]
        // while (dataFileReader.hasNext) {
        //   user = dataFileReader.next(user)
        //   System.err.println("Read " + user.getSchema().getName() + " record from Avro: " + user)
        // }
        // sb.result()
      }

      // Map each impression to a (post_id, 1) tuple so we can reduce by key to count the impressions
      val postCounts = impressions.map(row => (row._1, 1)).reduceByKey(_ + _)

      // Incorporate this batch into the long-running state
      val postCountStateStream = postCounts.mapWithState(postStateSpec)
      postCountStateStream.print()

      // Output the current snapshot state
      val postCountStateSnapshotStream = postCountStateStream.stateSnapshots()
      postCountStateSnapshotStream.foreachRDD { rdd =>
        println("Top Posts: -------------------------------")
        rdd.top(10)(Ordering[Long].on(_._2)).foreach(println)
      }

      // Map each post to a (author_id, 1) tuple so we can reduce by key to count the impressions
      val authorCounts = impressions.map(row => (row._2, 1)).reduceByKey(_ + _)

      // Incorporate this batch into the long-running state
      val authorCountStateStream = authorCounts.mapWithState(authorStateSpec)
      authorCountStateStream.print()

      // Output the current snapshot state
      val authorCountStateSnapshotStream = authorCountStateStream.stateSnapshots()
      authorCountStateSnapshotStream.foreachRDD { rdd =>
        println("Top Authors: -------------------------------")
        rdd.top(10)(Ordering[Long].on(_._2)).foreach(println)
      }

      streamingContext
    })

    // Start the streaming context and await termination
    ssc.start()
    ssc.awaitTermination()
  }

  def trackStateFunc(batchTime: Time, key: String, value: Option[Int], state: State[Long]): Option[(String, Long)] = {
    val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
    val output = (key, sum)
    state.update(sum)
    Some(output)
  }
}
