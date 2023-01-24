# Kafka Retry With Non-Blocking Delay Project

Spring Boot application demonstrating usage of a pattern for Kafka consumer retry with non-blocking delay.

## Build
```
mvn clean install
```

## Run Spring Boot Application

### Run docker containers

From root dir run the following to start dockerised Kafka, Zookeeper, and Confluent Control Center:
```
docker-compose up -d
```

### Start demo spring boot application
```
java -jar target/kafka-retry-with-delay-1.0.0.jar
```

### Produce a send-payment command event:

Jump onto Kafka docker container:
```
docker exec -ti kafka bash
```

Produce a demo-inbound message:
```
kafka-console-producer \
--topic demo-inbound-topic \
--broker-list kafka:29092 \
--property "key.separator=:" \
--property parse.key=true
```
Now enter the message (with key prefix):
```
"my-key":{"id": "123-abc", "data": "my-data"}
```
The demo-inbound message is consumed by the application...... TODO

## Integration Tests

Run integration tests with `mvn clean test`

The tests demonstrate sending events to an embedded in-memory Kafka that are consumed by the application, .......  TODO
