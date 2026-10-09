import json
import logging
import os
import time

from kafka import KafkaProducer

from app.db import connect


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("outbox-relay")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "orders.events")
POLL_SECONDS = float(os.getenv("RELAY_POLL_SECONDS", "1"))


def publish_one(producer: KafkaProducer) -> bool:
    event = None
    try:
        with connect() as connection:
            with connection.transaction():
                event = connection.execute(
                    """
                    SELECT id, aggregate_id, event_type, payload, created_at
                    FROM outbox_events
                    WHERE published_at IS NULL
                    ORDER BY created_at
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    """
                ).fetchone()
                if event is None:
                    return False

                message = {
                    "event_id": str(event["id"]),
                    "event_type": event["event_type"],
                    "occurred_at": event["created_at"].isoformat(),
                    "data": event["payload"],
                }
                producer.send(
                    TOPIC,
                    key=str(event["aggregate_id"]).encode("utf-8"),
                    value=json.dumps(message).encode("utf-8"),
                ).get(timeout=15)
                connection.execute(
                    """
                    UPDATE outbox_events
                    SET published_at = now(), last_error = NULL
                    WHERE id = %s
                    """,
                    (event["id"],),
                )
        logger.info("Published event %s", event["id"])
        return True
    except Exception as exc:
        if event is not None:
            try:
                with connect() as connection:
                    connection.execute(
                        """
                        UPDATE outbox_events
                        SET attempts = attempts + 1, last_error = %s
                        WHERE id = %s
                        """,
                        (str(exc)[:2000], event["id"]),
                    )
            except Exception:
                logger.exception("Could not record relay failure for event %s", event["id"])
        logger.exception("Outbox relay publish failed")
        return False


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        acks="all",
        retries=5,
        key_serializer=None,
        value_serializer=None,
    )
    logger.info("Outbox relay started; publishing to %s", TOPIC)
    try:
        while True:
            if not publish_one(producer):
                time.sleep(POLL_SECONDS)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
