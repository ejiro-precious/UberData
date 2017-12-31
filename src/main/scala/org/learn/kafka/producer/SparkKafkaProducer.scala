package org.learn.kafka.producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import java.util.concurrent.Future

class SparkKafkaProducer[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {

  // This is the key to work around into NotSerializableExceptions
  lazy val producer = createProducer()

  def send(topic: String, key: K, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, key, value))

  def send(topic: String, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, value))
}

object SparkKafkaProducer {

  import scala.collection.JavaConversions._

  def apply[K, V](config: Map[String, String]): SparkKafkaProducer[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config)

      sys.addShutdownHook {
        // Ensure before JVM shutdown , the kafka producer sends any buffered msg to kafka before shutting down
        producer.close()
      }

      producer
    }

    new SparkKafkaProducer(createProducerFunc)
  }

  def apply[K, V](config: java.util.Properties): SparkKafkaProducer[K, V] = apply(config.toMap)
}
