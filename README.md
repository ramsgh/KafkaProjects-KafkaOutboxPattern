# Kafka Transactional Outbox Demo

A runnable, single-node Kafka demo showing how to persist business data and its event in the same PostgreSQL transaction, then publish that event asynchronously.

The reference article at <https://medium.com/@turkishtechnology/implementing-outbox-pattern-with-kafka-for-asynchronous-data-synchronization-3ba64316e08f> could not be retrieved from this environment, so this project implements the established outbox pattern independently rather than reproducing article text or code.

## Architecture

1. The React UI sends an order to the FastAPI producer.
2. The API inserts the order and an `OrderCreated` row in `outbox_events` within one PostgreSQL transaction.
3. The outbox relay claims unpublished rows with `FOR UPDATE SKIP LOCKED`, publishes them to Kafka topic `orders.events`, and marks them published.
4. The Kafka consumer persists events to `consumed_events`. Its primary key on `event_id` makes a redelivery harmless.
5. The dashboard polls the API to display saved orders and consumed events.

The relay is at-least-once: a process failure after Kafka accepts a message but before PostgreSQL records `published_at` can cause a duplicate publish. The consumer's event ID constraint provides idempotent processing for this demo.

## Run

Requires Docker with the Compose plugin.

```sh
docker compose up --build
```

Then open:

- Dashboard: <http://localhost:5173>
- API documentation: <http://localhost:8000/docs>
- PostgreSQL: `localhost:5432` (`outbox` / `outbox`, database `outboxdb`)
- Kafka: `localhost:9094` from the host, or `kafka:9092` from a Compose service (topic `orders.events`)

The API and background workers start after PostgreSQL is healthy; the relay and consumer also wait for the Kafka topic initialization container.

If the first PostgreSQL initialization takes a while, wait for it to finish and check `docker compose ps`. The healthcheck allows a longer first-start window. If initialization previously stopped after creating the database and schema, start the stack again with `docker compose up --build`; the named volume preserves that data, so do not use `docker compose down -v` unless you intentionally want to erase it.

To stop:

```sh
docker compose down
```

To remove local database and Kafka data too:

```sh
docker compose down -v
```

The default credentials and open Kafka listener are for local development only. Replace them and configure authentication/TLS before exposing the stack outside a trusted development network.
