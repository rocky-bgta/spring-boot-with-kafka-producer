# Spring Boot Kafka Producer Demo

A small **Spring Boot 2.7 / Java 8 source-compatible** producer example for the [matching consumer demo](https://github.com/rocky-bgta/spring-boot-with-kafka-consumer). The GitHub Actions unit-test job runs on Java 17. This is a learning project, not a payment system.

## Message flow

```text
HTTP POST /paytm/payment
  -> PaytmController
  -> generate a separate PaymentRequest for each event
  -> Spring Kafka JsonSerializer / KafkaTemplate
  -> payment-topic (Kafka)
  -> spring-boot-with-kafka-consumer
```

**Why separate objects?** KafkaTemplate sends asynchronously. Reusing and mutating the same request object across sends risks serializing later state into earlier events. Each generated event now has its own transaction ID, account fields, amount and timestamp; the caller's input is not modified.

## Prerequisites and configuration

- JDK 8+ and Maven (CI tests with Java 17).
- A Kafka broker reachable at `localhost:9092` by default.
- Topic `payment-topic` created in your broker, or broker-side topic auto-creation enabled.

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap address |
| `PAYMENT_TOPIC` | `payment-topic` | Topic to publish |
| `PAYMENT_BATCH_SIZE` | `500` | Number of generated payment events per POST |

Create the topic with a Kafka CLI installation (adjust the bootstrap address for your setup):

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic payment-topic --partitions 3 --replication-factor 1
```

Start the application:

```bash
mvn spring-boot:run
```

## Try the endpoints

```bash
curl -X POST http://localhost:9191/paytm/payment \
  -H "Content-Type: application/json" \
  -d '{"payload":{"transactionId":"seed","srcAc":"seed","destAc":"seed","amount":1}}'
```

The payload is a demo request wrapper; the controller generates the configured number of synthetic events rather than processing a real payment. To try a smaller batch, set `PAYMENT_BATCH_SIZE=3` before starting the app.

`GET /publish/{message}` is a **legacy bulk-load demo** that publishes 10,001 string messages per call. It should not be used as a production API; the paired consumer expects payment objects, so use the POST endpoint for the end-to-end payment example.

## Verification and limitations

```bash
mvn -B -Dtest=PaytmControllerTest test
```

The isolated JUnit/Mockito test checks that published objects are distinct, have unique transaction IDs, and do not mutate the original request. It does not require a running Kafka broker. **Returning from the HTTP endpoint means the asynchronous sends were initiated, not that Kafka acknowledged delivery.** Broker failures, retries, idempotency, backpressure, and end-to-end integration testing are not handled here.
