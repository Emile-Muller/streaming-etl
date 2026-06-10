import json
import logging
import time
import os

from confluent_kafka import Consumer, KafkaException
from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _require_env(key: str) -> str:
	value = os.getenv(key)
	if value is None:
		raise RuntimeError(f"Required environment variable '{key}' is not set.")
	return value

KAFKA_TOPIC = "ecommerce_events"
KAFKA_CONFIG: dict[str, str] = {
	"bootstrap.servers": _require_env("KAFKA_BOOTSTRAP_SERVERS"),
	"group.id": "es-consumer-group",
	"auto.offset.reset": "earliest"
}

ES_URL: str = "http://elasticsearch:9200"
ES_INDEX: str = "raw_events"

def wait_for_elasticsearch(
	es: Elasticsearch,
	retries: int = 10,
	delay: float = 5.0
) -> None:
	"""
	Block until Elasticsearch is reachable, retrying on failure.

	Attempts to ping ``es`` up to ``retries`` times, sleeping ``delay``
	seconds between each unsuccessful attempt.
	"""
	for attempt in range(retries):
		try:
			if es.ping():
				logger.info("Connected to Elasticsearch")
				return
		except Exception:
			pass
		logger.warning(f"Elasticsearch not ready, retrying in {delay}s (attempt {attempt + 1}/{retries})")
		time.sleep(delay)
	raise RuntimeError("Could not connect to Elasticsearch after multiple retries")

def insert_to_elasticsearch() -> None:
	"""
	Consume events from Kafka and index them into Elasticsearch one by one.

	Creates a Kafka consumer subscribed to ``ecommerce_events`` and an
	Elasticsearch client pointed at ``ES_URL``, waits for Elasticsearch to
	become reachable via :func:`wait_for_elasticsearch`, then runs an
	infinite poll loop that decodes each message as JSON and indexes it
	into ``ES_INDEX``.

	Each event is indexed immediately (no batching). The consumer is always
	closed on exit regardless of the exit reason.
	"""
	consumer: Consumer = Consumer(KAFKA_CONFIG)
	consumer.subscribe([KAFKA_TOPIC])
	
	es: Elasticsearch = Elasticsearch(ES_URL)
	wait_for_elasticsearch(es)
	
	try:
		while True:
			msg = consumer.poll(1.0)
			
			if msg is None:
				continue
			elif msg.error():
				raise KafkaException(msg.error())
				
			event: dict = json.loads(msg.value().decode("utf-8"))
			es.index(index=ES_INDEX, document=event)
			
	except KeyboardInterrupt:
		logger.info("Interrupted by user, shutting down...")

	except KafkaException as e:
		logger.error(f"Kafka error: {e}")

	except Exception as e:
		logger.error(f"Unexpected error: {e}")

	finally:
		consumer.close()
		logger.info("Consumer closed.")

if __name__ == "__main__":
	insert_to_elasticsearch()