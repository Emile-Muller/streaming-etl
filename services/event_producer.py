import json
import logging
import time
import math
import os

import numpy as np
import pandas as pd
from psycopg2.extensions import connection as PgConnection
import psycopg2
from confluent_kafka import Producer

from session_generator import generate_user_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
KAFKA_CONFIG: dict[str, str | int] = {
	"bootstrap.servers": _require_env("KAFKA_BOOTSTRAP_SERVERS"),
	"linger.ms": 10,
	"batch.size": 16384,
	"client.id": "event-generator",
}
	
def sanitize(obj):
	"""
	Coerce a single value into a JSON-safe Python scalar.
	
	Handles native Python scalars and their NumPy equivalents.
	``NaN`` floats (both ``float`` and ``np.floating``) are converted to
	``None`` so the downstream ``json.dumps`` call can enforce
	``allow_nan=False`` without raising.
	"""
	if isinstance(obj, bool):
		return obj
	if isinstance(obj, (int, float)):
		return None if (isinstance(obj, float) and math.isnan(obj)) else obj
	if isinstance(obj, str):
		return obj
	if isinstance(obj, (np.floating,)):
		return None if np.isnan(obj) else float(obj)
	if isinstance(obj, (np.integer,)):
		return int(obj)
	if isinstance(obj, (np.bool_,)):
		return bool(obj)
	raise TypeError(f"Type {type(obj)} not serializable")
	
def json_serializer(event: dict) -> bytes:
	"""
	Serialize an event dictionary to UTF-8-encoded JSON.

	Each value is passed through :func:`sanitize` before serialization.
	``allow_nan=False`` is enforced so that any unsanitized ``NaN`` or
	``Inf`` causes an explicit error rather than silent invalid JSON.
	"""
	return json.dumps(
		{k: sanitize(v) for k, v in event.items()},
		allow_nan=False
	).encode("utf-8")

def delivery_report(err, msg) -> None:
	"""
	Kafka producer delivery callback.

	Logs a warning if the message failed to be delivered.
	Successful deliveries are silently ignored.
	"""
	if err is not None:
		logger.error(f"Delivery failed: {err}")

def produce_kafka_events(df_product: pd.DataFrame, df_user: pd.DataFrame) -> None:
	"""
	Continuously generate user sessions and publish their events to Kafka.
	
	Runs an infinite loop that:
	
		1. Generates a session via :func:`~session_generator.generate_user_session`.
		2. Serializes each event with :func:`json_serializer`, keyed by ``user_id``.
		3. Publishes to ``ecommerce_events`` with a 1-second inter-event delay.
		4. Registers :func:`delivery_report` as the per-message delivery callback.
		
	The loop exits cleanly on ``KeyboardInterrupt``; any other exception is
	logged and also terminates the loop. The producer is always flushed on
	exit regardless of the exit reason.
	"""
	producer = Producer(KAFKA_CONFIG)

	try:
		while True:
			session_events: list[dict] = generate_user_session(df_product, df_user)

			for event in session_events:
				producer.produce(
					topic=KAFKA_TOPIC,
					key=event["user_id"].encode("utf-8"),
					value=json_serializer(event),
					callback=delivery_report
				)
				producer.poll(0)
				time.sleep(1)

	except KeyboardInterrupt:
		logger.info("Interrupted by user, shutting down...")
	except Exception as e:
		logger.error(f"Unexpected error: {e}")
	finally:
		producer.flush()
		logger.info("Producer closed.")
	
def start_producer() -> None:
	"""
	Entry point: load dimension tables from PostgreSQL, then start the producer.
	
	Opens a connection to fetch ``dim.products_features`` and
	``dim.users`` into DataFrames, closes the connection, then hands the
	DataFrames off to :func:`produce_kafka_events`.
	"""
	conn: PgConnection = psycopg2.connect(**PG_CONFIG)
	
	try:
		df_product: pd.DataFrame = pd.read_sql("SELECT * FROM dim.products_features", conn)
		df_user: pd.DataFrame = pd.read_sql("SELECT * FROM dim.users", conn)
	finally:
		conn.close()
	
	produce_kafka_events(df_product, df_user)
	
if __name__ == "__main__":
	start_producer()