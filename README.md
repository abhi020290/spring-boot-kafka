# spring-boot-kafka

# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties
.\bin\windows\zookeeper-server-start.bat config\zookeeper.properties
# Start Kafka Server
bin/kafka-server-start.sh config/server.properties
.\bin\windows\kafka-server-start.sh config\server.properties
# Create Kafka Topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Kafka_Example
.\bin\windows\kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic NewTopic
# Consume from the Kafka Topic via Console
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic Kafka_Example --from-beginning
.\bin\windows\kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic NewTopic --from-beginning


# Docker Compose

Execute below command to start zooker and broker both using docker-compose

docker-compose up -d