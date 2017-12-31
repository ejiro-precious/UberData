package org.learn.kafka.producer

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.connect.json.JsonSerializer
import org.apache.kafka.clients.producer.{ProducerConfig, RecordMetadata}
import org.apache.spark.broadcast.Broadcast
import org.codehaus.jackson.map.ObjectMapper
import java.util.concurrent.Future

import org.codehaus.jackson.JsonNode

case class Item(id: Int, value: Int) extends Serializable {
  override def toString: String = {
    s"""{"id":"${id}", "value":"${value}"}"""
  }
}

object KafkaProducer extends App {

  if (args.length != 3) {
    println("Usage : [sparkApp] topic bootstrap numberMessages")
    sys.exit(1)
  }

  // bootstrap: localhost:9092
  // zookeeper: localhost:2181
  val Array(topic, bootstrap, messages) = args

  // Setting Spark
  val ssc: StreamingContext = {
    val sparkConf = new SparkConf().setAppName("UberProducer")
    new StreamingContext(sparkConf, Seconds(2))
  }

  // Setting kafka producer
  val kafkaProducer: Broadcast[SparkKafkaProducer[String, String]] = {
  //val kafkaProducer: Broadcast[SparkKafkaProducer[String, JsonNode]] = {

    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "uber_kafka")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    //properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer")

    //org.apache.kafka.connect.json.JsonSerializer
    ssc.sparkContext.broadcast(SparkKafkaProducer[String, String](properties))
    //ssc.sparkContext.broadcast(SparkKafkaProducer[String, JsonNode](properties))
  }

  val items = (0 until messages.toInt).map(i => Item(i, i))
  val itemsRDD = ssc.sparkContext.parallelize(items)

  // ConstantInputDStream is a stream where always return the same stream, useful for testing
  val dStream = new ConstantInputDStream[Item](ssc, itemsRDD)

  // Send the different elements to kafka
  val objectMapper = new ObjectMapper()
  dStream.foreachRDD(rdd => {

    println("####################################################")

    rdd.foreachPartition(partition => {

      println("*************************************************************")

      //val producer = new KafkaProducer[String, JsonSerializer](properties)

      val metadata: Stream[Future[RecordMetadata]] = partition.map{ record =>

        //val jsonNode = objectMapper.valueToTree(record)
        //val message = new ProducerRecord[String, JsonSerializer](topic, record.id.toString, jsonNode)
        //  val message = new ProducerRecord[String, String](topic, record.id.toString, record.toString)

        kafkaProducer.value.send(topic, record.id.toString, record.toString)
        //kafkaProducer.value.send(topic, record.id.toString, jsonNode)
      }.toStream

      //metadata.foreach{ metadataRow =>
     //   metadataRow.get()
     // }
    })
  })

  ssc.start()
  ssc.awaitTermination()
}
