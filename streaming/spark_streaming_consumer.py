"""
PySpark Structured Streaming Consumer

Reads events from Kafka, deserializes JSON payloads, applies watermarking
for late data handling, performs window-based aggregations, handles bad records,
and writes to Delta Lake with checkpointing for exactly-once semantics.

Usage:
    spark-submit --master spark://master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0 \
        streaming/spark_streaming_consumer.py \
        --bootstrap-servers kafka:9092 \
        --topic events \
        --output-path s3a://data-lake/streaming/events \
        --checkpoint-path s3a://data-lake/checkpoints/events
"""

import argparse
import logging
import sys
from typing import List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, BooleanType, TimestampType,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("spark_streaming_consumer")


# ---------------------------------------------------------------------------
# Event Schema
# ---------------------------------------------------------------------------
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=True),
    StructField("session_id", StringType(), nullable=True),
    StructField("timestamp", StringType(), nullable=False),
    StructField("page_url", StringType(), nullable=True),
    StructField("referrer_url", StringType(), nullable=True),
    StructField("device_type", StringType(), nullable=True),
    StructField("os", StringType(), nullable=True),
    StructField("browser", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("duration_ms", LongType(), nullable=True),
    StructField("revenue", DoubleType(), nullable=True),
    StructField("is_conversion", BooleanType(), nullable=True),
])


# ---------------------------------------------------------------------------
# Stream Processing Functions
# ---------------------------------------------------------------------------
def read_from_kafka(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "latest",
    max_offsets_per_trigger: int = 100000,
    kafka_options: Optional[dict] = None,
) -> DataFrame:
    """
    Create a streaming DataFrame from a Kafka topic.

    Returns a DataFrame with Kafka metadata and raw value bytes.
    """
    reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .option("kafka.group.id", f"spark-consumer-{topic}")
        .option("kafka.auto.offset.reset", "latest")
        .option("kafka.enable.auto.commit", "false")
    )

    if kafka_options:
        for k, v in kafka_options.items():
            reader = reader.option(k, v)

    return reader.load()


def deserialize_events(raw_df: DataFrame, schema: StructType) -> DataFrame:
    """
    Deserialize Kafka message values from JSON bytes to structured columns.

    Bad records are routed to a separate error column for dead-letter handling.
    """
    deserialized = (
        raw_df
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_json"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("timestampType").alias("kafka_timestamp_type"),
        )
        .withColumn(
            "parsed",
            F.from_json(F.col("raw_json"), schema),
        )
        .withColumn(
            "is_valid",
            F.col("parsed").isNotNull() & F.col("parsed.event_id").isNotNull(),
        )
    )

    return deserialized


def process_valid_events(df: DataFrame) -> DataFrame:
    """
    Process valid events: extract fields, parse timestamps, add metadata.
    """
    return (
        df
        .filter(F.col("is_valid") == True)
        .select(
            F.col("parsed.event_id").alias("event_id"),
            F.col("parsed.event_type").alias("event_type"),
            F.col("parsed.user_id").alias("user_id"),
            F.col("parsed.session_id").alias("session_id"),
            F.to_timestamp(
                F.col("parsed.timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"
            ).alias("event_timestamp"),
            F.col("parsed.page_url").alias("page_url"),
            F.col("parsed.referrer_url").alias("referrer_url"),
            F.col("parsed.device_type").alias("device_type"),
            F.col("parsed.os").alias("os"),
            F.col("parsed.browser").alias("browser"),
            F.col("parsed.country").alias("country"),
            F.col("parsed.city").alias("city"),
            F.col("parsed.duration_ms").alias("duration_ms"),
            F.col("parsed.revenue").alias("revenue"),
            F.col("parsed.is_conversion").alias("is_conversion"),
            F.col("kafka_timestamp").alias("kafka_ingested_at"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
        )
        .withColumn("processing_time", F.current_timestamp())
        .withColumn("event_date", F.to_date("event_timestamp"))
    )


def extract_bad_records(df: DataFrame) -> DataFrame:
    """Extract invalid/unparseable records for dead-letter handling."""
    return (
        df
        .filter(F.col("is_valid") == False)
        .select(
            F.col("kafka_key"),
            F.col("raw_json"),
            F.col("kafka_topic"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
            F.col("kafka_timestamp"),
            F.current_timestamp().alias("error_timestamp"),
            F.lit("parse_error").alias("error_type"),
        )
    )


def apply_watermark_and_window_aggregations(
    events_df: DataFrame,
    watermark_duration: str = "10 minutes",
    window_duration: str = "5 minutes",
    slide_duration: str = "1 minute",
) -> DataFrame:
    """
    Apply watermarking for late data and compute window-based aggregations.

    Produces per-window metrics:
    - Event count by type
    - Total revenue
    - Unique users
    - Average duration
    """
    return (
        events_df
        .withWatermark("event_timestamp", watermark_duration)
        .groupBy(
            F.window("event_timestamp", window_duration, slide_duration),
            "event_type",
        )
        .agg(
            F.count("event_id").alias("event_count"),
            F.sum("revenue").alias("total_revenue"),
            F.approx_count_distinct("user_id").alias("unique_users"),
            F.avg("duration_ms").alias("avg_duration_ms"),
            F.max("event_timestamp").alias("latest_event"),
            F.sum(F.when(F.col("is_conversion"), 1).otherwise(0)).alias("conversion_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_type",
            "event_count",
            "total_revenue",
            "unique_users",
            "avg_duration_ms",
            "latest_event",
            "conversion_count",
            F.current_timestamp().alias("computed_at"),
        )
    )


def apply_stateful_session_tracking(events_df: DataFrame) -> DataFrame:
    """
    Stateful processing: maintain running session metrics per user.

    Uses groupBy with watermark to track active sessions.
    """
    return (
        events_df
        .withWatermark("event_timestamp", "30 minutes")
        .groupBy(
            F.col("user_id"),
            F.session_window("event_timestamp", "30 minutes"),
        )
        .agg(
            F.count("event_id").alias("session_events"),
            F.min("event_timestamp").alias("session_start"),
            F.max("event_timestamp").alias("session_end"),
            F.sum("revenue").alias("session_revenue"),
            F.first("device_type").alias("device_type"),
            F.first("country").alias("country"),
        )
        .select(
            "user_id",
            F.col("session_window.start").alias("session_start"),
            F.col("session_window.end").alias("session_end"),
            "session_events",
            "session_revenue",
            "device_type",
            "country",
            F.current_timestamp().alias("computed_at"),
        )
    )


# ---------------------------------------------------------------------------
# Write Sinks
# ---------------------------------------------------------------------------
def write_to_delta(
    df: DataFrame,
    output_path: str,
    checkpoint_path: str,
    trigger: str = "processingTime",
    trigger_interval: str = "30 seconds",
    partition_cols: Optional[List[str]] = None,
    output_mode: str = "append",
    query_name: str = "delta_sink",
):
    """
    Write a streaming DataFrame to Delta Lake with checkpointing.

    Supports processingTime and once trigger modes.
    """
    writer = (
        df.writeStream
        .format("delta")
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint_path)
        .queryName(query_name)
    )

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    if trigger == "once":
        writer = writer.trigger(once=True)
    elif trigger == "availableNow":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=trigger_interval)

    # Delta-specific options
    writer = writer.option("mergeSchema", "true")

    query = writer.start(output_path)
    logger.info("Started streaming query '%s' writing to %s.", query_name, output_path)
    return query


def write_bad_records(
    df: DataFrame,
    output_path: str,
    checkpoint_path: str,
):
    """Write bad records to a dead-letter path."""
    return (
        df.writeStream
        .format("json")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
        .queryName("dead_letter_sink")
        .trigger(processingTime="60 seconds")
        .start()
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming Kafka consumer.")
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic", default="events")
    parser.add_argument("--output-path", required=True, help="Delta Lake output path.")
    parser.add_argument("--checkpoint-path", required=True, help="Checkpoint location.")
    parser.add_argument("--trigger", default="processingTime", choices=["processingTime", "once", "availableNow"])
    parser.add_argument("--trigger-interval", default="30 seconds")
    parser.add_argument("--watermark", default="10 minutes")
    parser.add_argument("--starting-offsets", default="latest")
    parser.add_argument("--enable-aggregations", action="store_true")
    parser.add_argument("--enable-sessions", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)

    sys.path.insert(0, "spark-jobs/utils")
    from spark_session import SparkSessionBuilder

    spark = (
        SparkSessionBuilder(app_name="StreamingConsumer")
        .with_delta()
        .with_s3()
        .with_config("spark.sql.streaming.schemaInference", "false")
        .with_config("spark.sql.streaming.checkpointLocation", args.checkpoint_path)
        .build()
    )

    try:
        # Read from Kafka
        raw_stream = read_from_kafka(
            spark,
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            starting_offsets=args.starting_offsets,
        )

        # Deserialize
        deserialized = deserialize_events(raw_stream, EVENT_SCHEMA)

        # Process valid events
        events = process_valid_events(deserialized)

        # Write valid events to Delta Lake
        events_query = write_to_delta(
            events,
            output_path=f"{args.output_path}/raw_events",
            checkpoint_path=f"{args.checkpoint_path}/raw_events",
            trigger=args.trigger,
            trigger_interval=args.trigger_interval,
            partition_cols=["event_date", "event_type"],
            query_name="events_to_delta",
        )

        # Write bad records
        bad_records = extract_bad_records(deserialized)
        bad_query = write_bad_records(
            bad_records,
            output_path=f"{args.output_path}/dead_letter",
            checkpoint_path=f"{args.checkpoint_path}/dead_letter",
        )

        queries = [events_query, bad_query]

        # Optional: Window aggregations
        if args.enable_aggregations:
            agg_df = apply_watermark_and_window_aggregations(
                events, watermark_duration=args.watermark,
            )
            agg_query = write_to_delta(
                agg_df,
                output_path=f"{args.output_path}/aggregations",
                checkpoint_path=f"{args.checkpoint_path}/aggregations",
                trigger=args.trigger,
                trigger_interval=args.trigger_interval,
                output_mode="update",
                query_name="aggregations_to_delta",
            )
            queries.append(agg_query)

        # Optional: Session tracking
        if args.enable_sessions:
            session_df = apply_stateful_session_tracking(events)
            session_query = write_to_delta(
                session_df,
                output_path=f"{args.output_path}/sessions",
                checkpoint_path=f"{args.checkpoint_path}/sessions",
                trigger=args.trigger,
                trigger_interval=args.trigger_interval,
                output_mode="update",
                query_name="sessions_to_delta",
            )
            queries.append(session_query)

        logger.info("All streaming queries started. Awaiting termination.")

        # Wait for all queries
        spark.streams.awaitAnyTermination()

    except Exception:
        logger.exception("Streaming consumer failed.")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
