package com.fyber

import java.lang

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Kafka consumer handler , by default the consumer read data from kafka default topic ("consumer_offsets")
  * @param topicName kafka topic name
  * @param streamingContext spark streaming
  * @param sparkSession spark session
  * @param conf Typesafe configuration
  */
class KafkaToS3Stream(topicName: String,
                      streamingContext: StreamingContext,
                      sparkSession: SparkSession)(implicit conf:Config) extends LazyLogging{

  // C'tor
  logger.info(s"Initializing stream for topic $topicName")
  private val finalPartitionsNumber      = conf.getInt("direct.final.files.number")
  private val bucketName                 = conf.getString("direct.s3.bucket.name")
  private val groupId                    = conf.getString("direct.kafka.group.id.prefix")
  private val destPath                   = s"s3a://$bucketName/$topicName"
  private val sumOffsetsPartitionName    = conf.getString("direct.partition.by.dimentions.offsets")

  // init topic schema in order to convert RDD to DataFrame
  private val schema = generateSchema

  // init spark stream to consume kafka events
  private val stream = generateStream(streamingContext)

  // stream main operation
  stream.foreachRDD(x=>forEachRDDFunc(x))


  def forEachRDDFunc(rdd: RDD[ConsumerRecord[String,String]]): Unit = {

    // get all partitions offsets in the micro batch
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    if(!rdd.isEmpty()) {
      val sumOffsets = getSumOffsets(offsetRanges)
      val df = rddToDataFrame(sparkSession, rdd)

      // add unique identifier to the DataFrame in order to use it during writing the data (partitionBy) - Lazy spark action
      val dfWithPartitionId = df.withColumn(sumOffsetsPartitionName, lit(sumOffsets))
      val partitionByDimension = getEventDimensionsToPartitionBy :+ sumOffsetsPartitionName


      // cache is needed to improve performance as are going to:
      // 1) read all data in order to calculate the path where its going to be written , so we can overwrite this path
      // 2) read all data in order to write it to sink (s3)
      dfWithPartitionId.cache()

      deleteDestPathsOnS3(dfWithPartitionId, partitionByDimension)
      writeDF(dfWithPartitionId, partitionByDimension)

      dfWithPartitionId.unpersist()

      commitOffsetsToKafka(offsetRanges)
    }else{
      logger.warn(s"No data to consume from kafka")
    }
  }


  // convert schema file to Spark StructType
  private def generateSchema : StructType = {
    val schemaFileName   = s"${topicName}_schema.json"
    val schemaJsonString = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(schemaFileName)).mkString
    DataType.fromJson(schemaJsonString).asInstanceOf[StructType]
  }

  private def generateStream(streamingContext: StreamingContext) = {
    KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](List(topicName), createKafkaParams)
    )
  }


  // convert RDD to DataFrame
  // adding repartition (in our use case its not balanced among the partitions)
  // repartition value is according to final number of files we want in the sink (s3)
  private def rddToDataFrame(spark: SparkSession, rdd: RDD[ConsumerRecord[String, String]]) = {
    import spark.implicits._
    val valuesRDD : RDD[String]     = rdd.map(_.value())
    val repartitionedRDD            = valuesRDD.repartition(finalPartitionsNumber)
    val dataSet   : Dataset[String] = spark.sqlContext.createDataset(repartitionedRDD)
    val dataFrame                   = spark.read.schema(schema).json(dataSet)
    dataFrame
  }

  // sum 'fromOffset' from all partitions in order to have unique identifier of the micro batch
  private def getSumOffsets(offsetRanges: Array[OffsetRange]): String = {
    offsetRanges.foldLeft(BigInt(0)) {
      (sum, or) => sum + BigInt(or.fromOffset)
    }.toString
  }


  // calculate destination path from the DataFrame of the current micro batch
  private def getDestPaths(df: org.apache.spark.sql.DataFrame, partitionsSeq: Seq[String]): List[String]= {
    import scala.collection.JavaConversions._

    df.select(partitionsSeq.head, partitionsSeq.tail: _*)
      .distinct().collectAsList()
      .map(row => {
        val partitionDimensionToValue: Seq[(String, String)] =
          row
            .getValuesMap(partitionsSeq)
            .toSeq

        val fullPath = partitionDimensionToValue.map {
          case (partitionBy, value) => s"$partitionBy=$value"
        }.mkString("/")
        fullPath
      }).toList
  }

  // overwrite the sink destination by writing empty DataFrame
  private def deleteDestPathsOnS3(df: DataFrame, partitionsSeq: Seq[String]): Unit = {
    getDestPaths(df, partitionsSeq)
      .map(path => s"$topicName/$path")
      .foreach(path =>
        sparkSession.emptyDataFrame.write.mode(SaveMode.Overwrite).save(s"s3a://$bucketName/$path")
      )
  }

  private def writeDF(df: DataFrame, partitionByDimension: Seq[String]): Unit = {
    df.write
      .mode(SaveMode.Append)
      .partitionBy(partitionByDimension: _*)
      .option("compression", "gzip")
      .parquet(destPath)
    logger.info(s"Wrote to $destPath")
  }

  protected def getEventDimensionsToPartitionBy: Seq[String] =
    conf.getStringList("direct.partition.by.dimentions.event")


  // kafka consumer configuration
  def createKafkaParams: Map[String, Object] = {
    val sessionTimeOutMs = conf.getInt("direct.batch.duration.seconds")*1000
    val requestTimeoutMs = sessionTimeOutMs * 2
    Map[String, Object](
      "bootstrap.servers"  -> getKafkaServersList.mkString(","),
      "key.deserializer"   -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id"           -> s"${groupId}_$topicName",
      "session.timeout.ms" -> sessionTimeOutMs.toString,
      "request.timeout.ms" -> requestTimeoutMs.toString,
      "enable.auto.commit" -> (false: lang.Boolean),
      "auto.offset.reset"  -> conf.getString("direct.kafka.starting.offset"),
      "rebalance.max.retries" -> conf.getInt("direct.kafka.rebalance.max.retries").toString,
      "retry.backoff.ms" -> conf.getInt("direct.kafka.retry.backoff.ms").toString,
      "refresh.leader.backoff.ms" -> conf.getInt("direct.kafka.refresh.leader.backoff.ms").toString
    )
  }

  def getKafkaServersList: List[String] = {
    val kafkaServersConfigs = conf.getConfigList("kafka.serversList")
    kafkaServersConfigs
      .map(serverConf => {
        val host = serverConf.getString("host")
        val port = serverConf.getInt("port")
        s"$host:$port"
      }).toList
  }

  // async write offsets to kafka at the end of each micro batch
  private def commitOffsetsToKafka(offsetRanges: Array[OffsetRange]): Unit = {
    logger.info(s"Commiting offsets to kafka")
    stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
  }

}
