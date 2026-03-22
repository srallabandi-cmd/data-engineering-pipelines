"""
Kafka Event Producer
====================

Production Kafka producer that publishes user events with:
- JSON serialisation with schema validation
- Delivery callbacks with success/error tracking
- Key-based partitioning for ordering guarantees
- Configurable batching (linger.ms, batch.size)
- Metrics tracking (produced, delivered, failed)
- Graceful shutdown on SIGINT/SIGTERM

Usage:
    python streaming/kafka_producer.py \
        --bootstrap-servers localhost:9092 \
        --topic user-events \
        --rate 100
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from threading import Event
from typing import Any, Dict, List, Optional

from confluent_kafka import KafkaError, KafkaException, Producer

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("kafka_producer")

# ---------------------------------------------------------------------------
# Shutdown signal
# ---------------------------------------------------------------------------
_shutdown = Event()


def _signal_handler(signum, frame):
    logger.info("Received signal %d — initiating graceful shutdown", signum)
    _shutdown.set()


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ---------------------------------------------------------------------------
# Event schema
# ---------------------------------------------------------------------------
EVENT_SCHEMA_FIELDS = {
    "event_id": str,
    "user_id": str,
    "event_type": str,
    "event_timestamp": str,
    "event_properties": dict,
}

VALID_EVENT_TYPES = [
    "page_view",
    "click",
    "purchase",
    "signup",
    "logout",
    "search",
    "add_to_cart",
    "remove_from_cart",
    "checkout",
]


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
class ProducerMetrics:
    """Thread-safe metrics tracker for the Kafka producer."""

    def __init__(self) -> None:
        self.produced = 0
        self.delivered = 0
        self.failed = 0
        self._start_time = time.monotonic()

    def record_produced(self) -> None:
        self.produced += 1

    def record_delivered(self) -> None:
        self.delivered += 1

    def record_failed(self) -> None:
        self.failed += 1

    @property
    def elapsed_seconds(self) -> float:
        return time.monotonic() - self._start_time

    @property
    def throughput(self) -> float:
        elapsed = self.elapsed_seconds
        return self.delivered / elapsed if elapsed > 0 else 0.0

    def summary(self) -> Dict[str, Any]:
        return {
            "produced": self.produced,
            "delivered": self.delivered,
            "failed": self.failed,
            "elapsed_seconds": round(self.elapsed_seconds, 2),
            "throughput_per_sec": round(self.throughput, 2),
        }


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------
def validate_event(event: Dict[str, Any]) -> bool:
    """Validate that an event dict conforms to the expected schema."""
    for field, expected_type in EVENT_SCHEMA_FIELDS.items():
        if field not in event:
            logger.warning("Missing required field: %s", field)
            return False
        if not isinstance(event[field], expected_type):
            logger.warning(
                "Field '%s' expected %s, got %s",
                field,
                expected_type.__name__,
                type(event[field]).__name__,
            )
            return False
    if event.get("event_type") not in VALID_EVENT_TYPES:
        logger.warning("Invalid event_type: %s", event.get("event_type"))
        return False
    return True


# ---------------------------------------------------------------------------
# Event generator (for demo / testing)
# ---------------------------------------------------------------------------
def generate_event(user_pool_size: int = 1000) -> Dict[str, Any]:
    """Generate a synthetic user event for testing."""
    import random

    user_id = f"user_{random.randint(1, user_pool_size):05d}"
    event_type = random.choice(VALID_EVENT_TYPES)
    pages = ["/home", "/products", "/cart", "/checkout", "/profile", "/search"]

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_type": event_type,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "event_properties": {
            "page": random.choice(pages),
            "referrer": random.choice(["google", "direct", "email", "social"]),
            "device": random.choice(["mobile", "desktop", "tablet"]),
            "duration_seconds": round(random.uniform(0.5, 300.0), 2),
        },
        "session_id": f"sess_{random.randint(1, 100000):06d}",
        "platform": random.choice(["web", "ios", "android"]),
        "app_version": random.choice(["2.1.0", "2.2.0", "2.3.0"]),
    }


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------
class EventProducer:
    """High-throughput Kafka producer with delivery tracking."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        schema_registry_url: Optional[str] = None,
        extra_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.topic = topic
        self.metrics = ProducerMetrics()

        config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": f"event-producer-{os.getpid()}",
            # Batching
            "linger.ms": 10,
            "batch.size": 65536,
            "batch.num.messages": 500,
            # Reliability
            "acks": "all",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 5,
            "retries": 10,
            "retry.backoff.ms": 100,
            # Compression
            "compression.type": "lz4",
            # Monitoring
            "statistics.interval.ms": 30000,
        }

        if extra_config:
            config.update(extra_config)

        self._producer = Producer(config, logger=logger)
        logger.info(
            "Producer initialised: servers=%s topic=%s",
            bootstrap_servers,
            topic,
        )

    def _delivery_callback(self, err, msg):
        """Called once for each message produced to indicate delivery result."""
        if err is not None:
            self.metrics.record_failed()
            logger.error(
                "Delivery failed for key=%s: %s",
                msg.key().decode("utf-8") if msg.key() else "N/A",
                err,
            )
        else:
            self.metrics.record_delivered()

    def produce(self, event: Dict[str, Any]) -> bool:
        """Serialise and produce a single event to Kafka.

        Returns True if the event was enqueued successfully.
        """
        if not validate_event(event):
            self.metrics.record_failed()
            return False

        key = event["user_id"].encode("utf-8")
        value = json.dumps(event).encode("utf-8")

        try:
            self._producer.produce(
                topic=self.topic,
                key=key,
                value=value,
                callback=self._delivery_callback,
            )
            self.metrics.record_produced()
            return True

        except BufferError:
            logger.warning("Producer queue full — flushing and retrying")
            self._producer.flush(timeout=5)
            try:
                self._producer.produce(
                    topic=self.topic,
                    key=key,
                    value=value,
                    callback=self._delivery_callback,
                )
                self.metrics.record_produced()
                return True
            except Exception as exc:
                logger.error("Failed to produce after flush: %s", exc)
                self.metrics.record_failed()
                return False

        except KafkaException as exc:
            logger.error("Kafka error: %s", exc)
            self.metrics.record_failed()
            return False

    def produce_batch(self, events: List[Dict[str, Any]]) -> int:
        """Produce a batch of events. Returns the number successfully enqueued."""
        success_count = 0
        for event in events:
            if self.produce(event):
                success_count += 1
            # Poll to trigger delivery callbacks without blocking
            self._producer.poll(0)
        return success_count

    def flush(self, timeout: float = 30.0) -> int:
        """Flush all buffered messages. Returns remaining messages."""
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning("%d messages still in queue after flush", remaining)
        return remaining

    def close(self) -> None:
        """Gracefully shut down the producer."""
        logger.info("Shutting down producer — flushing remaining messages")
        remaining = self.flush(timeout=30.0)
        if remaining > 0:
            logger.warning("Abandoned %d undelivered messages", remaining)
        logger.info("Producer metrics: %s", json.dumps(self.metrics.summary()))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka event producer")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--topic", default="user-events")
    parser.add_argument(
        "--rate", type=int, default=100, help="Events per second (0 = unlimited)"
    )
    parser.add_argument(
        "--total", type=int, default=0, help="Total events to produce (0 = infinite)"
    )
    parser.add_argument(
        "--user-pool", type=int, default=1000, help="Number of simulated users"
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)

    producer = EventProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
    )

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    count = 0

    try:
        while not _shutdown.is_set():
            event = generate_event(user_pool_size=args.user_pool)
            producer.produce(event)
            count += 1

            if count % 1000 == 0:
                producer._producer.poll(0)
                logger.info("Produced %d events — %s", count, json.dumps(producer.metrics.summary()))

            if 0 < args.total <= count:
                logger.info("Reached target of %d events", args.total)
                break

            if interval > 0:
                time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
