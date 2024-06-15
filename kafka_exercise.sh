#create new topic named weather
docker exec -it mykafkaserver /opt/kafka/bin/kafka-topics.sh --create --topic weather --bootstrap-server localhost:9092

#post messages to topic weather
docker exec -it mykafkaserver /opt/kafka/bin/kafka-console-producer.sh   --bootstrap-server localhost:9092   --topic weather

#Read the messages from the topic 
docker exec -it mykafkaserver /opt/kafka/bin/kafka-console-consumer.sh   --bootstrap-server localhost:9092   --topic weather
