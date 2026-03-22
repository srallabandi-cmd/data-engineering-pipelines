"""
Production Kafka Producer

Publishes events to Kafka topics with:
- JSON schema validation before publishing
- Delivery callbacks for guaranteed delivery tracking
- Configurable batching for throughput optimization
- Key-based partitioning for ordering guarantees
- Graceful shutdown with flush on SIGTERM/SIGINT
- Metrics reporting (messages sent, errors, latency)
- Retry logic for transient failures

Usage:
    python streaming/kafka_producer.py \
        --bootstrap-servers kafka:9092 \
        --topic events \
        --schema-registry-url http://schema-registry:8081
"""

import argparse
import atexit
import json
import logging
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("kafka_producer")


# ---------------------------------------------------------------------------
# Schema Validation
# ---------------------------------------------------------------------------
EVENT_SCHEMA = {
    "required_fields": {"event_id", "event_type", "timestamp"},
    "allowed_event_types": {
        "page_view", "click", "scroll", "purchase", "signup", "login", "logout",
    },
    "string_fields": {
        "event_id", "event_type", "user_id", "session_id",
        "page_url", "referrer_url", "device_type", "os", "browser",
        "country", "city",
    },
    "numeric_fields": {"duration_ms", "revenue"},
}


def validate_event(event: Dict[str, Any]) -> bool:
    """
    Validate an event against the schema.

    Returns True if valid; raises ValueError with details if invalid.
    """
    missing = EVENT_SCHEMA["required_fields"] - set(event.keys())
    if missing:
        raise ValueError(f"Missing required fields: {missing}")

    event_type = event.get("event_type")
    if event_type not in EVENT_SCHEMA["allowed_event_types"]:
        raise ValueError(
            f"Invalid event_type '{event_type}'. "
            f"Allowed: {EVENT_SCHEMA['allowed_event_types']}"
        )

    for field_name in EVENT_SCHEMA["string_fields"]:
        if field_name in event and event[field_name] is not None:
            if not isinstance(event[field_name], str):
                raise ValueError(f"Field '{field_name}' must be a string, got {type(event[field_name]).__name__}.")

    for field_name in EVENT_SCHEMA["numeric_fields"]:
        if field_name in event and event[field_name] is not None:
            if not isinstance(event[field_name], (int, float)):
                raise ValueError(f"Field '{field_name}' must be numeric, got {type(event[field_name]).__name__}.")
            if event[field_name] < 0:
                raise ValueError(f"Field '{field_name}' must be non-negative, got {event[field_name]}.")

    return True


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
@dataclass
class ProducerMetrics:
    """Track producer performance metrics."""
    messages_sent: int = 0
    messages_failed: int = 0
    messages_validated: int = 0
    validation_errors: int = 0
    total_bytes_sent: int = 0
    delivery_latency_sum: float = 0.0
    delivery_latency_count: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def avg_delivery_latency_ms(self) -> float:
        if self.delivery_latency_count == 0:
            return 0.0
        return (self.delivery_latency_sum / self.delivery_latency_count) * 1000

    @property
    def throughput_mps(self) -> float:
        elapsed = time.time() - self.start_time
        if elapsed == 0:
            return 0.0
        return self.messages_sent / elapsed

    def summary(self) -> Dict[str, Any]:
        return {
            "messages_sent": self.messages_sent,
            "messages_failed": self.messages_failed,
            "validation_errors": self.validation_errors,
            "avg_delivery_latency_ms": round(self.avg_delivery_latency_ms, 2),
            "throughput_mps": round(self.throughput_mps, 1),
            "total_bytes_sent": self.total_bytes_sent,
            "uptime_seconds": round(time.time() - self.start_time, 1),
        }


# ---------------------------------------------------------------------------
# Event Producer
# ---------------------------------------------------------------------------
class EventProducer:
    """
    Production Kafka event producer with delivery guarantees.

    Features:
    - Synchronous schema validation before produce
    - Asynchronous delivery with callbacks
    - Key-based partitioning (by user_id or event_type)
    - Configurable batching (linger.ms, batch.size)
    - Graceful shutdown with flush
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        partition_key: str = "user_id",
        enable_idempotence: bool = True,
        batch_size: int = 65536,
        linger_ms: int = 10,
        compression: str = "lz4",
        acks: str = "all",
        retries: int = 5,
        retry_backoff_ms: int = 100,
        extra_config: Optional[Dict[str, Any]] = None,
    ):
        self.topic = topic
        self.partition_key = partition_key
        self.metrics = ProducerMetrics()
        self._running = True

        config = {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": enable_idempotence,
            "acks": acks,
            "retries": retries,
            "retry.backoff.ms": retry_backoff_ms,
            "batch.size": batch_size,
            "linger.ms": linger_ms,
            "compression.type": compression,
            "max.in.flight.requests.per.connection": 5,
            "queue.buffering.max.messages": 100000,
            "queue.buffering.max.kbytes": 1048576,
            "message.max.bytes": 1048576,
            "statistics.interval.ms": 60000,
            "error_cb": self._error_callback,
            "stats_cb": self._stats_callback,
        }

        if extra_config:
            config.update(extra_config)

        self._producer = Producer(config)
        self._string_serializer = StringSerializer("utf-8")

        # Register graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        atexit.register(self.close)

        logger.info(
            "EventProducer initialized: servers=%s, topic=%s, compression=%s, acks=%s",
            bootstrap_servers, topic, compression, acks,
        )

    # ---- Callbacks -------------------------------------------------------

    def _delivery_callback(self, err, msg):
        """Called once per message to indicate delivery result."""
        if err is not None:
            logger.error(
                "Message delivery failed: topic=%s, partition=%s, error=%s",
                msg.topic(), msg.partition(), err,
            )
            self.metrics.messages_failed += 1
        else:
            self.metrics.messages_sent += 1
            self.metrics.total_bytes_sent += len(msg.value() or b"")

            latency = msg.latency()
            if latency is not None:
                self.metrics.delivery_latency_sum += latency
                self.metrics.delivery_latency_count += 1

    def _error_callback(self, err):
        """Called on producer-level errors."""
        logger.error("Producer error: %s", err)

    def _stats_callback(self, stats_json: str):
        """Called periodically with producer statistics."""
        stats = json.loads(stats_json)
        logger.debug(
            "Producer stats: msg_cnt=%s, msg_size=%s, txmsgs=%s",
            stats.get("msg_cnt"), stats.get("msg_size"), stats.get("txmsgs"),
        )

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info("Received signal %d. Initiating graceful shutdown.", signum)
        self._running = False

    # ---- Produce ---------------------------------------------------------

    def produce(self, event: Dict[str, Any], validate: bool = True) -> bool:
        """
        Produce a single event to the Kafka topic.

        Parameters
        ----------
        event : dict
            Event data to publish.
        validate : bool
            Whether to validate against the schema before producing.

        Returns
        -------
        bool
            True if the message was accepted by the producer buffer.
        """
        if validate:
            try:
                validate_event(event)
                self.metrics.messages_validated += 1
            except ValueError as exc:
                logger.warning("Validation failed: %s. Event: %s", exc, event.get("event_id"))
                self.metrics.validation_errors += 1
                return False

        # Determine partition key
        key = event.get(self.partition_key, event.get("event_id", str(uuid.uuid4())))
        key_bytes = self._string_serializer(key)

        # Serialize value
        value = json.dumps(event, default=str).encode("utf-8")

        try:
            self._producer.produce(
                topic=self.topic,
                key=key_bytes,
                value=value,
                on_delivery=self._delivery_callback,
                headers={
                    "content-type": "application/json",
                    "produced-at": datetime.now(timezone.utc).isoformat(),
                    "source": "data-engineering-pipelines",
                },
            )
            self._producer.poll(0)  # Trigger delivery callbacks
            return True

        except BufferError:
            logger.warning("Producer buffer full. Flushing and retrying.")
            self._producer.flush(timeout=5.0)
            try:
                self._producer.produce(
                    topic=self.topic,
                    key=key_bytes,
                    value=value,
                    on_delivery=self._delivery_callback,
                )
                return True
            except Exception as exc:
                logger.error("Produce failed after flush: %s", exc)
                self.metrics.messages_failed += 1
                return False

        except KafkaException as exc:
            logger.error("Kafka produce error: %s", exc)
            self.metrics.messages_failed += 1
            return False

    def produce_batch(self, events: List[Dict[str, Any]], validate: bool = True) -> int:
        """
        Produce a batch of events.

        Returns the number of successfully buffered messages.
        """
        success_count = 0
        for event in events:
            if self.produce(event, validate=validate):
                success_count += 1

        # Trigger callbacks for the batch
        self._producer.poll(0)
        return success_count

    def flush(self, timeout: float = 30.0) -> int:
        """Flush the producer buffer. Returns the number of messages still in queue."""
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning("%d messages still in queue after flush (timeout=%.1fs).", remaining, timeout)
        return remaining

    def close(self) -> None:
        """Gracefully shut down the producer."""
        logger.info("Shutting down producer. Flushing remaining messages.")
        remaining = self.flush(timeout=30.0)
        if remaining > 0:
            logger.warning("Producer closed with %d messages still pending.", remaining)
        logger.info("Producer metrics: %s", json.dumps(self.metrics.summary()))

    @property
    def is_running(self) -> bool:
        return self._running


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka event producer.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="events")
    parser.add_argument("--partition-key", default="user_id")
    parser.add_argument("--batch-size", type=int, default=65536)
    parser.add_argument("--linger-ms", type=int, default=10)
    parser.add_argument("--compression", default="lz4", choices=["none", "gzip", "snappy", "lz4", "zstd"])
    parser.add_argument("--demo", action="store_true", help="Run demo mode with synthetic events.")
    parser.add_argument("--demo-count", type=int, default=1000, help="Number of demo events to produce.")
    return parser.parse_args(argv)


def generate_demo_event() -> Dict[str, Any]:
    """Generate a synthetic event for demo/testing."""
    import random
    import string

    event_types = ["page_view", "click", "scroll", "purchase", "signup", "login", "logout"]
    devices = ["mobile", "desktop", "tablet"]
    countries = ["US", "UK", "DE", "FR", "JP", "BR", "IN"]

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(event_types),
        "user_id": f"user_{random.randint(1, 10000):05d}",
        "session_id": f"sess_{''.join(random.choices(string.hexdigits, k=12))}",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "page_url": f"https://example.com/page/{random.randint(1, 100)}",
        "referrer_url": random.choice([None, "https://google.com", "https://twitter.com"]),
        "device_type": random.choice(devices),
        "os": random.choice(["iOS", "Android", "Windows", "macOS", "Linux"]),
        "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
        "country": random.choice(countries),
        "city": "SomeCity",
        "duration_ms": random.randint(100, 60000),
        "revenue": round(random.uniform(0, 500), 2) if random.random() < 0.1 else 0.0,
        "is_conversion": random.random() < 0.05,
    }


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)

    producer = EventProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        partition_key=args.partition_key,
        batch_size=args.batch_size,
        linger_ms=args.linger_ms,
        compression=args.compression,
    )

    if args.demo:
        logger.info("Running demo mode: producing %d events.", args.demo_count)
        events = [generate_demo_event() for _ in range(args.demo_count)]
        sent = producer.produce_batch(events)
        producer.flush()
        logger.info("Demo complete. %d/%d events sent.", sent, args.demo_count)
        logger.info("Metrics: %s", json.dumps(producer.metrics.summary()))
    else:
        logger.info("Producer running. Reading events from stdin (JSON per line).")
        try:
            while producer.is_running:
                line = sys.stdin.readline()
                if not line:
                    break
                try:
                    event = json.loads(line.strip())
                    producer.produce(event)
                except json.JSONDecodeError:
                    logger.warning("Invalid JSON: %s", line.strip()[:200])
        except KeyboardInterrupt:
            pass
        finally:
            producer.close()


if __name__ == "__main__":
    main()
