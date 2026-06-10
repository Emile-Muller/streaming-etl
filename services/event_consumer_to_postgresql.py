import json
import logging
import time
import os

import psycopg2
from psycopg2.extensions import connection as PgConnection, cursor as PgCursor
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BATCH_SIZE: int = 100
FLUSH_INTERVAL: float = 1.0  # seconds

def _require_env(key: str) -> str:
	value = os.getenv(key)
	if value is None:
		raise RuntimeError(f"Required environment variable '{key}' is not set.")
	return value

PG_CONFIG: dict[str, str | int] = {
	"host": _require_env("POSTGRES_HOST"),
	"port": int(_require_env("POSTGRES_PORT")),
	"database": _require_env("POSTGRES_DB"),
	"user": _require_env("POSTGRES_USER"),
	"password": _require_env("POSTGRES_PASSWORD"),
}

KAFKA_TOPIC = "ecommerce_events"
KAFKA_CONFIG: dict[str, str | bool] = {
	"bootstrap.servers": _require_env("KAFKA_BOOTSTRAP_SERVERS"),
	"group.id": "postgres-consumer-group",
	"auto.offset.reset": "earliest",
	"enable.auto.commit": False
}

def flush_buffer(
	cursor: PgCursor,
	conn: PgConnection,
	consumer: Consumer,
	buffer: list[tuple],
	offsets: list
) -> None:
	"""
	Flush the in-memory event buffer to PostgreSQL and commit Kafka offsets.

	The write is intentionally ordered — DB commit first, Kafka offset commit
	second — so that a crash between the two causes re-delivery rather than
	silent data loss.

	No-ops silently when ``buffer`` is empty.
	"""
	if not buffer:
		return

	execute_values(
		cursor,
		"""
		INSERT INTO raw.events (
			event_id,
			user_id,
			session_id,
			product_id,
			event_type,
			price_unit,
			discount_pct,
			quantity,
			price_total,
			timestamp,
			raw_payload
		)
		VALUES %s
		ON CONFLICT (event_id) DO NOTHING
		""",
		buffer
	)
	
	conn.commit()
	
	# commit Kafka offsets only after DB success
	consumer.commit(asynchronous=False)
	
	logger.info(f"Flushed {len(buffer)} events")
	
	buffer.clear()
	offsets.clear()

def insert_to_postgres() -> None:
	"""
	Consume events from Kafka and persist them to ``raw.events`` in batches.

	Opens a single PostgreSQL connection and a Kafka consumer subscribed to ``ecommerce_events``, then runs an infinite poll loop that:

		1. Decodes each message as JSON.
		2. Skips messages missing ``event_id`` (logged as a warning); logs a
		warning for messages missing ``event_type`` but still buffers them.
		3. Appends a row tuple and the raw ``Message`` to ``buffer`` / ``offsets``.
		4. Calls :func:`flush_buffer` when ``buffer`` reaches ``BATCH_SIZE`` or
		``FLUSH_INTERVAL`` seconds have elapsed since the last flush.

	On exit (``KeyboardInterrupt``, ``KafkaException``, or any unexpected
	exception) a final :func:`flush_buffer` is attempted before the consumer,
	cursor, and connection are all closed.
	"""
	conn: PgConnection = psycopg2.connect(**PG_CONFIG)

	conn.autocommit = False
	cursor: PgCursor = conn.cursor()

	consumer: Consumer = Consumer(KAFKA_CONFIG)
	consumer.subscribe([KAFKA_TOPIC])
	logger.info("Consumer started...")
	
	buffer: list[tuple] = []
	offsets: list = []
	last_flush: float = time.time()

	try:
		while True:
			msg = consumer.poll(1.0)

			if msg is None:
				continue
			elif msg.error():
				raise KafkaException(msg.error())
				
			event: dict = json.loads(msg.value().decode("utf-8"))
			
			if not event.get("event_id"):
				logger.warning(f"Skipping event missing event_id: {event}")
				continue
			
			if not event.get("event_type"):
				logger.warning(f"Event missing event_type: {event.get('event_id')}")

			buffer.append((
				event.get("event_id"),
				event.get("user_id"),
				event.get("session_id"),
				event.get("product_id"),
				event.get("event_type"),
				event.get("price_unit"),
				event.get("discount_pct"),
				event.get("quantity"),
				event.get("price_total"),
				event.get("timestamp"),
				json.dumps(event)
			))
			
			offsets.append(msg)

			now: float = time.time()

			if len(buffer) >= BATCH_SIZE or (now - last_flush) >= FLUSH_INTERVAL:
				flush_buffer(cursor, conn, consumer, buffer, offsets)
				last_flush = now
				
	except KeyboardInterrupt:
		logger.info("Interrupted by user, shutting down...")

	except KafkaException as e:
		logger.error(f"Kafka error: {e}")

	except Exception as e:
		logger.error(f"Unexpected error: {e}")

	finally:
		flush_buffer(cursor, conn, consumer, buffer, offsets)
		consumer.close()
		cursor.close()
		conn.close()
		logger.info("Consumer closed.")

if __name__ == "__main__":
	insert_to_postgres()