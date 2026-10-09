import json
import logging
import os
import time
from uuid import UUID

from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata
from psycopg.types.json import Jsonb

from app.db import connect


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("orders-consumer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "orders.events")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "orders-dashboard")


def persist_event(message: dict) -> None:
    event_id = UUID(message["event_id"])
    with connect() as connection:
        connection.execute(
            """
            INSERT INTO consumed_events (event_id, event_type, payload)
            VALUES (%s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (event_id, message["event_type"], Jsonb(message)),
        )


def main():
    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
                group_id=GROUP_ID,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
            )
            logger.info("Consuming topic %s", TOPIC)
            for record in consumer:
                try:
                    message = json.loads(record.value.decode("utf-8"))
                    persist_event(message)
                    consumer.commit(
                        {
                            TopicPartition(record.topic, record.partition): OffsetAndMetadata(
                                record.offset + 1,
                                None,
                                -1,
                            )
                        }
                    )
                    logger.info("Consumed event %s", message["event_id"])
                except Exception:
                    logger.exception(
                        "Could not process Kafka record at partition=%s offset=%s",
                        record.partition,
                        record.offset,
                    )
                    raise
        except Exception:
            logger.exception("Kafka consumer failed; retrying shortly")
            if consumer is not None:
                consumer.close()
            time.sleep(3)
        else:
            consumer.close()


if __name__ == "__main__":
    main()
