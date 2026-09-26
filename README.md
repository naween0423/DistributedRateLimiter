# Kafka Rate Limiter

A Spring Boot application that publishes records to Kafka and consumes them with a Redis-backed rate limit. Failed records are retried and eventually sent to a dead-letter topic.

## Requirements

- Java 17
- Maven 3.6 or later
- Docker and Docker Compose

## Start Kafka and Redis

From the project root:

```bash
docker compose up -d
```

The Compose file starts ZooKeeper, Kafka, and Redis. Kafka is exposed at `localhost:9092` and Redis at `localhost:6379`.

## Run the application

With Kafka and Redis running, launch the Spring Boot application from the project root:

```bash
mvn spring-boot:run
```

On startup, the `testProducer` runner sends 25 sample records to the `records-to-process` Kafka topic. The consumer listens to that topic using the `external-api-workers` consumer group.

To build and run the packaged application instead:

```bash
mvn package
java -jar target/kafka-rate-limiter-0.0.1-SNAPSHOT.jar
```

## Processing behavior

- The producer creates records with status `PENDING`.
- The consumer checks the shared Redis rate-limit bucket before processing each record. The default limit is 100 calls per minute.
- `ExternalApiService` is currently a placeholder: it logs the record and waits briefly rather than calling a real external service.
- Successful processing changes the record status to `COMPLETED` in memory. The updated record is not written back to the input topic.
- Failures are retried up to three times. Retry records are sent to `records-to-process`; after the retry limit, the record is sent to `records-dlq`.

Kafka retains the original messages, so inspecting `records-to-process` may still show the original `PENDING` status even after a record has been processed. Check the application logs for consumer processing results.

## Verify Kafka

List topics:

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

The `records-to-process` topic is created when the application first publishes to it. The `records-dlq` topic is used if a record reaches the retry limit.

Read records from the beginning of the input topic:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic records-to-process \
  --from-beginning
```

## Configuration

Runtime settings are in `src/main/resources/application.yml`:

| Setting | Default |
| --- | --- |
| Kafka bootstrap server | `localhost:9092` |
| Redis host and port | `localhost:6379` |
| Application port | `8080` |
| Rate-limit capacity | `100` |
| Rate-limit refill interval | `1` minute |

Override these values with Spring Boot configuration or environment variables as needed. For example, set `SPRING_KAFKA_BOOTSTRAP_SERVERS` and `SPRING_DATA_REDIS_HOST` when running in a container. The supplied Compose file only starts Kafka and Redis; it does not define an application container. If the app runs in a separate container, configure Kafka's advertised listener so that `kafka:9092` (or the chosen broker address) is reachable by both the app and your Kafka client.

## Project layout

- `KafkaRateLimiterApplication` — Spring Boot entry point and startup sample producer
- `producer/RecordProducer` — publishes records to Kafka
- `consumer/RecordConsumer` — consumes, rate-limits, retries, and routes exhausted records to the DLQ
- `service/RateLimiterService` — uses Bucket4j and Redis for the shared rate-limit bucket
- `service/ExternalApiService` — placeholder external API integration
- `model/RecordItem` — Kafka record data model
