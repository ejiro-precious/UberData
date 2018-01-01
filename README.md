# Uber Streaming

Project working with Spark Streaming, Kafka and avro.

The first stage using spark streaming pull some message in avro format to kafka


## Kafka
	spark-submit --driver-memory 1g --executor-memory 1g --class org.learn.kafka.producer.KafkaProducer UberSpark.jar uber_stream localhost:9092 100

	./kafka-console-consumer.sh --zookeeper localhost:2181 --topic uber_stream --from-beginning

