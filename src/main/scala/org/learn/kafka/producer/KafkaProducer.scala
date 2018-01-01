package org.learn.kafka.producer

import java.io.ByteArrayOutputStream
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.producer.{ProducerConfig, RecordMetadata}
import org.apache.spark.broadcast.Broadcast
import org.codehaus.jackson.map.ObjectMapper
import java.util.concurrent.Future

import org.apache.avro.Schema.Parser
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter

import scala.io.Source

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

  //Read avro schema file and create generic record structure
  val schema: Schema = new Parser().parse(Source.fromURL(getClass.getResource("/schema.avsc")).mkString)
  val genericItem: GenericRecord = new GenericData.Record(schema)

  // Create anonymous function
  val itemByteArray = (item: GenericRecord) => {
    val writer = new SpecificDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(out, null)
    writer.write(item, encoder)
    encoder.flush()
    out.close()

    out.toByteArray
  }

  // Setting kafka producer
  //val kafkaProducer: Broadcast[SparkKafkaProducer[String, String]] = {
  val kafkaProducer: Broadcast[SparkKafkaProducer[String, Array[Byte]]] = {

    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "uber_kafka")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    //properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

    //ssc.sparkContext.broadcast(SparkKafkaProducer[String, String](properties))
    ssc.sparkContext.broadcast(SparkKafkaProducer[String, Array[Byte]](properties))
  }

  val items = (0 until messages.toInt).map(i => Item(i, i))
  val itemsRDD = ssc.sparkContext.parallelize(items)

  // ConstantInputDStream is a stream where always return the same stream, useful for testing
  val dStream = new ConstantInputDStream[Item](ssc, itemsRDD)

  // Send the different elements to kafka
  val objectMapper = new ObjectMapper()
  dStream.foreachRDD(rdd => {

    rdd.foreachPartition(partition => {

      val metadata: Stream[Future[RecordMetadata]] = partition.map{ record =>

        // val message = new ProducerRecord[String, String](topic, record.id.toString, record.toString)

        // Create Record
        genericItem.put("id", record.id)
        genericItem.put("value", record.value)

        // Serialize generic record to byte array
        val serializedBytes = itemByteArray(genericItem)

        println(s"****** Printing record: id: $record.id - value: $record.value")

        kafkaProducer.value.send(topic, record.id.toString, serializedBytes)
        //kafkaProducer.value.send(topic, record.id.toString, record.toString)
      }.toStream

      //metadata.foreach{ metadataRow =>
     //   metadataRow.get()
     // }
    })
  })

  ssc.start()
  ssc.awaitTermination()
}
