from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
import logging
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from psycopg.types.json import Jsonb

from app.db import connect


logger = logging.getLogger(__name__)


class CreateOrder(BaseModel):
    customer_name: str = Field(min_length=1, max_length=120)
    amount: Decimal = Field(gt=0, max_digits=12, decimal_places=2)


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        with connect() as connection:
            connection.execute("SELECT 1")
    except Exception as exc:
        raise RuntimeError("Could not connect to PostgreSQL") from exc
    yield


app = FastAPI(
    title="Kafka Transactional Outbox Demo",
    description="Create orders in PostgreSQL and asynchronously publish their outbox events to Kafka.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/api/health")
def health():
    try:
        with connect() as connection:
            connection.execute("SELECT 1")
        return {"status": "ok"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Database is unavailable") from exc


@app.post("/api/orders", status_code=201)
def create_order(order: CreateOrder):
    order_id = uuid4()
    event_id = uuid4()
    created_at = datetime.now(timezone.utc)
    payload = {
        "order_id": str(order_id),
        "customer_name": order.customer_name.strip(),
        "amount": str(order.amount),
        "created_at": created_at.isoformat(),
    }
    if not payload["customer_name"]:
        raise HTTPException(status_code=422, detail="Customer name cannot be blank")

    try:
        with connect() as connection:
            with connection.transaction():
                connection.execute(
                    """
                    INSERT INTO orders (id, customer_name, amount, created_at)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (order_id, payload["customer_name"], order.amount, created_at),
                )
                connection.execute(
                    """
                    INSERT INTO outbox_events
                        (id, aggregate_type, aggregate_id, event_type, payload)
                    VALUES (%s, 'order', %s, 'OrderCreated', %s)
                    """,
                    (event_id, order_id, Jsonb(payload)),
                )
    except Exception as exc:
        logger.exception("Could not save order")
        raise HTTPException(status_code=503, detail="Could not save order") from exc

    return {
        "id": str(order_id),
        "customer_name": payload["customer_name"],
        "amount": str(order.amount),
        "created_at": created_at.isoformat(),
        "outbox_event_id": str(event_id),
    }


@app.get("/api/orders")
def list_orders():
    with connect() as connection:
        return connection.execute(
            """
            SELECT id, customer_name, amount, created_at
            FROM orders
            ORDER BY created_at DESC
            LIMIT 50
            """
        ).fetchall()


@app.get("/api/events")
def list_consumed_events():
    with connect() as connection:
        return connection.execute(
            """
            SELECT event_id, event_type, payload, consumed_at
            FROM consumed_events
            ORDER BY consumed_at DESC
            LIMIT 50
            """
        ).fetchall()
