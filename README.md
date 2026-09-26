# Kafka Rate Limiter

A Spring Boot application that publishes records to Kafka and consumes them with a Redis-backed rate limit. Processing status is persisted in PostgreSQL. Failed records are retried and eventually sent to a dead-letter topic.

## Requirements

- Java 17
- Maven 3.6 or later
- Docker and Docker Compose

## Start Kafka, Redis, and PostgreSQL

From the project root:

```bash
docker compose up -d
```

The Compose file starts ZooKeeper, Kafka, Redis, and PostgreSQL. Kafka is exposed at `localhost:9092`, Redis at `localhost:6379`, and PostgreSQL at `localhost:5432`. PostgreSQL creates a database named `rate_limiter` with user `rate_limiter`; its data is retained in the `postgres_data` Docker volume.

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
- When the consumer receives a record, it inserts or updates the row in PostgreSQL, then updates that same row to `COMPLETED` after successful processing.
- The consumer checks the shared Redis rate-limit bucket before processing each record. The default limit is 100 calls per minute.
- `ExternalApiService` is currently a placeholder: it logs the record and waits briefly rather than calling a real external service.
- Failures are retried up to three times. The database row reflects `PENDING_RETRY` or `FAILED_DLQ` and the retry count; retry records are sent to `records-to-process`, and records reaching the retry limit are sent to `records-dlq`.

Kafka retains the original messages, so inspecting `records-to-process` may still show the original `PENDING` status even after processing. The PostgreSQL row is updated with the latest processing status.

## Inspect processed records

Connect to PostgreSQL with `psql` from the host:

```bash
psql -h localhost -p 5432 -U rate_limiter -d rate_limiter
```

The password is `rate_limiter_dev` for local development. Spring Boot creates the `processed_records` table automatically. Its columns mirror the Kafka message: `record_id` (primary key), `payload`, `status`, and `retry_count`.

Query the records:

```sql
SELECT record_id, payload, status, retry_count
FROM processed_records
ORDER BY record_id;
```

Alternatively, run the query from the PostgreSQL container:

```bash
docker exec -it postgres psql -U rate_limiter -d rate_limiter \
  -c "SELECT record_id, payload, status, retry_count FROM processed_records ORDER BY record_id;"
```

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
| PostgreSQL JDBC URL | `jdbc:postgresql://localhost:5432/rate_limiter` |
| PostgreSQL user/password | `rate_limiter` / `rate_limiter_dev` |
| Application port | `8080` |
| Rate-limit capacity | `100` |
| Rate-limit refill interval | `1` minute |

Override these values with Spring Boot configuration or environment variables as needed. When running the app in a container on the same Compose network, use `kafka:9092`, `redis`, and `jdbc:postgresql://postgres:5432/rate_limiter` instead of host `localhost` addresses. The supplied Compose file starts the dependencies but does not define an application container. If the app runs in a separate container, configure Kafka's advertised listener so that the broker address is reachable by both the app and your Kafka client.

## Project layout

- `KafkaRateLimiterApplication` — Spring Boot entry point and startup sample producer
- `producer/RecordProducer` — publishes records to Kafka
- `consumer/RecordConsumer` — consumes, rate-limits, retries, and routes exhausted records to the DLQ
- `service/RateLimiterService` — uses Bucket4j and Redis for the shared rate-limit bucket
- `service/ExternalApiService` — placeholder external API integration
- `repository/ProcessedRecordRepository` and `model/ProcessedRecord` — persist record data and processing status in PostgreSQL
- `model/RecordItem` — Kafka record data model
