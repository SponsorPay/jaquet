package com.fyber

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.JavaConversions._



/**
  * Main class
  */
object DirectStream extends LazyLogging {

  //C'tor
  private implicit val conf: Config      = ConfigFactory.load
  private val appName                    = conf.getString("direct.spark.spark.app.name")
  private val streamingTopics            = conf.getStringList("app.topics").toList
  private val streamBatchDurationSeconds = conf.getInt("direct.batch.duration.seconds")


  def main(args: Array[String]): Unit = {

    val sparkConf = createSparkConf(conf)
    val spark = buildSparkSession(sparkConf)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload","true")
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(streamBatchDurationSeconds))

    // init stream per topic
    streamingTopics.map {
      topicName => new KafkaToS3Stream(topicName, streamingContext, spark)
    }
    streamingContext.start()
    streamingContext.awaitTermination()
  }



  // init spark session
  private def buildSparkSession(sparkConf: SparkConf) = {
    SparkSession
      .builder()
      .appName(appName)
      .config(sparkConf)
      .getOrCreate()
  }

  // init sparkConf from configuration
  private def createSparkConf(config: Config): SparkConf = {
    config.getConfig("direct.spark").entrySet.foldLeft(new SparkConf) { case (sparkConf, entry) =>
      sparkConf.set(entry.getKey, entry.getValue.unwrapped.toString)
    }
  }

}
