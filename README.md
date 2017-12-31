# Uber Streaming


## Kafka
	spark-submit --driver-memory 1g --executor-memory 1g --class org.learn.kafka.producer.KafkaProducer UberSpark.jar uber_stream localhost:9092 100

	./kafka-console-consumer.sh --zookeeper localhost:2181 --topic uber_stream --from-beginning

