# Kafka: A Pattern For Retry With Non-Blocking Delay

Spring Boot application demonstrating usage of a pattern for Kafka consumer retry with non-blocking delay.

If an event is received by an application that is not ready to processit , it can be sent to a retry topic that will evaluate whether and when the event can be retried.

By passing the event off to a retry topic it means that other events on the same topic that potentially could be processed are not blocked.  If the event were to be repeatedly retried off the original topic then that blocks the events behind it.

The pattern allows a delay to be configured so that the events are not being continually retried, using up processing time and resources.  As the event can be retried many times, after a configurable period of time it will be discarded.  This ensures that it is not being retried forever (a poison pill), even if the application never moves to a state where it can process it.  When the retry evaluation determines that the event should be retried it is simply added back onto the original topic, so it will be re-consumed and the update will be attempted as before (which could result in another retry).  This ensures that the retry handling logic is encapsulated, has a single concern (the retry evaluation), having no knowledge of what the event is or the business logic required to process it.

For example, as this application demonstrates, an item is created with a create-item event, and updated with an update-item event.  If the update-item event is received before the create-item event it may be required to delay and retry this update after a period of time to allow for the corresponding create-item event to arrive and be processed.  When related events are originating in bulk from external systems it may well be the case that such events arrive out of order by the time they hit a downstream service.  This pattern therefore caters for such a scenario as the update-item event can be safely retried until the item is eventually created by the create-item event, at which point the update can be applied.

## Configuration

Configure the following properties in `src/main/resources/application.yml`:

|Property|Usage|Default|
|---|---|---|
|demo.retry.retryIntervalSeconds| The interval in seconds between retries| 10 seconds|
|demo.retry.maxRetryDurationSeconds| The maximum duration an event should be retried before being discarded|300 seconds|

## Build
```
mvn clean install
```

## Integration Tests

The integration tests run as part of the maven `test` target (during the `install`).

Configuration for the test is taken from the `src/test/resoures/application-test.yml`.

The tests demonstrate sending events to an embedded in-memory Kafka that are consumed by the application.  `create-item` events result in an item being persisted in the database.  `update-item` events update the corresponding item if it is present in the database.  The tests demonstrate that if the item is not found it is retried via the retry topic.

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

### Produce create and update item command events:

Jump onto Kafka docker container:
```
docker exec -ti kafka bash
```

Produce a message to the `create-item` topic:
```
kafka-console-producer \
--topic create-item \
--broker-list kafka:29092 
```
Now enter the message to create the item (with a UUID and name String):
```
{"id": "626bd1bd-c565-48ac-87b2-28f2247f6dea", "name": "my-new-item"}
```

Retrieve the item status via the REST API, confirming it is `NEW`:
```
curl -X GET http://localhost:9001/v1/demo/items/626bd1bd-c565-48ac-87b2-28f2247f6dea/status
```

Produce a message to the `update-item` topic:
```
kafka-console-producer \
--topic update-item \
--broker-list kafka:29092 
```

Enter the message to update the item. The status can be one of `ACTIVE` or `CANCELLED`
```
{"id": "626bd1bd-c565-48ac-87b2-28f2247f6dea", "status": "ACTIVE"}
```

Retrieve the updated item status via the REST API, confirming it is now `ACTIVE`:
```
curl -X GET http://localhost:9001/v1/demo/items/626bd1bd-c565-48ac-87b2-28f2247f6dea/status
```

### Exercise the retry with out of order events:

Submit an `update-item` first (with a different UUID, and status of `ACTIVE` or `CANCELLED`).  Observe that no item status is returned from the `curl` statement.  If a `create-item` event with this same itemId is submitted before the `maxRetryDurationSeconds` threshold is exceeded (as defined in `application.yml`), then the item will be created, and the retrying `update-item` event will transition the status to `ACTIVE` or `CANCELLED`.  If the threshold is exceeded then the status of the created item will remain at `NEW`.

### Docker clean up

Manual clean up:
```
docker rm -f $(docker ps -aq)
```
Further docker clean up if necessary:
```
docker system prune
docker volume prune
```
