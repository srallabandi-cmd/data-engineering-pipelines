"""
Event Transformation Module
============================

Production PySpark transformations for user event data including:
- Window functions (lag, lead, row_number, dense_rank)
- Session identification (30-minute inactivity gap)
- Multi-column aggregations
- Pivoting
- Broadcast join optimisation
- UDF registration and usage
- Data quality assertions
- Partitioned, repartitioned output

Usage:
    spark-submit --packages io.delta:delta-core_2.12:2.4.0 \
        spark-jobs/etl/transform_events.py \
        --input-path s3a://data-lake/raw/events \
        --output-path s3a://data-lake/transformed/events \
        --batch-date 2024-01-15
"""

import argparse
import hashlib
import logging
import sys
from datetime import datetime, timezone
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("transform_events")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SESSION_TIMEOUT_SECONDS = 30 * 60  # 30 minutes
OUTPUT_PARTITIONS = 8

# ---------------------------------------------------------------------------
# UDFs
# ---------------------------------------------------------------------------


def _anonymise_ip(ip: Optional[str]) -> Optional[str]:
    """Hash an IP address for privacy compliance."""
    if ip is None:
        return None
    return hashlib.sha256(ip.encode("utf-8")).hexdigest()[:16]


def _classify_device(user_agent: Optional[str]) -> str:
    """Classify device type from user-agent string."""
    if user_agent is None:
        return "unknown"
    ua_lower = user_agent.lower()
    if any(kw in ua_lower for kw in ("iphone", "android", "mobile")):
        return "mobile"
    if any(kw in ua_lower for kw in ("ipad", "tablet")):
        return "tablet"
    if any(kw in ua_lower for kw in ("bot", "crawler", "spider")):
        return "bot"
    return "desktop"


# ---------------------------------------------------------------------------
# Transformation steps
# ---------------------------------------------------------------------------


def read_source(spark: SparkSession, input_path: str, input_format: str) -> DataFrame:
    """Read raw events from Delta or Parquet source."""
    logger.info("Reading source data from %s (%s)", input_path, input_format)
    if input_format == "delta":
        df = spark.read.format("delta").load(input_path)
    else:
        df = spark.read.format("parquet").load(input_path)

    record_count = df.count()
    logger.info("Source contains %d records", record_count)
    return df


def deduplicate(df: DataFrame) -> DataFrame:
    """Remove exact duplicate events, keeping the earliest ingestion."""
    window = Window.partitionBy("event_id").orderBy(F.col("_extracted_at").asc())
    deduped = (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    removed = df.count() - deduped.count()
    if removed > 0:
        logger.info("Removed %d duplicate records", removed)
    return deduped


def enrich_with_udfs(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Apply UDFs for IP anonymisation and device classification."""
    anonymise_ip_udf = F.udf(_anonymise_ip, StringType())
    classify_device_udf = F.udf(_classify_device, StringType())

    return df.withColumn("ip_hash", anonymise_ip_udf(F.col("ip_address"))).withColumn(
        "device_category", classify_device_udf(F.col("user_agent"))
    )


def add_window_functions(df: DataFrame) -> DataFrame:
    """Add analytical columns using window functions.

    Columns added:
    - prev_event_type / next_event_type (lag / lead)
    - event_rank_per_user (dense_rank by timestamp)
    - seconds_since_last_event (time delta to prior event)
    """
    user_window = Window.partitionBy("user_id").orderBy("event_ts")

    return (
        df.withColumn("prev_event_type", F.lag("event_type", 1).over(user_window))
        .withColumn("next_event_type", F.lead("event_type", 1).over(user_window))
        .withColumn("event_rank_per_user", F.dense_rank().over(user_window))
        .withColumn("prev_event_ts", F.lag("event_ts", 1).over(user_window))
        .withColumn(
            "seconds_since_last_event",
            F.when(
                F.col("prev_event_ts").isNotNull(),
                F.unix_timestamp("event_ts") - F.unix_timestamp("prev_event_ts"),
            ).otherwise(F.lit(None)),
        )
    )


def sessionize(df: DataFrame) -> DataFrame:
    """Assign a session_id based on a 30-minute inactivity gap.

    A new session starts whenever the gap between consecutive events for the
    same user exceeds ``SESSION_TIMEOUT_SECONDS``.
    """
    user_window = Window.partitionBy("user_id").orderBy("event_ts")
    cumulative_window = (
        Window.partitionBy("user_id")
        .orderBy("event_ts")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return (
        df.withColumn(
            "is_new_session",
            F.when(
                F.col("seconds_since_last_event").isNull()
                | (F.col("seconds_since_last_event") > SESSION_TIMEOUT_SECONDS),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn("session_seq", F.sum("is_new_session").over(cumulative_window))
        .withColumn(
            "computed_session_id",
            F.concat_ws("_", F.col("user_id"), F.col("session_seq").cast("string")),
        )
        .drop("is_new_session", "session_seq")
    )


def compute_session_aggregates(df: DataFrame) -> DataFrame:
    """Calculate per-session aggregates joined back to event level."""
    session_stats = df.groupBy("user_id", "computed_session_id").agg(
        F.count("*").alias("session_event_count"),
        F.min("event_ts").alias("session_start"),
        F.max("event_ts").alias("session_end"),
        F.countDistinct("event_type").alias("session_distinct_event_types"),
        F.first("event_type").alias("session_entry_event"),
        F.last("event_type").alias("session_exit_event"),
    )
    session_stats = session_stats.withColumn(
        "session_duration_seconds",
        F.unix_timestamp("session_end") - F.unix_timestamp("session_start"),
    )

    return df.join(
        F.broadcast(session_stats),
        on=["user_id", "computed_session_id"],
        how="left",
    )


def pivot_event_counts(df: DataFrame) -> DataFrame:
    """Create a pivoted summary of event counts per user per date."""
    return (
        df.groupBy("user_id", "event_date")
        .pivot("event_type")
        .agg(F.count("*"))
        .na.fill(0)
    )


def assert_quality(df: DataFrame, label: str) -> None:
    """Run basic data quality assertions and log violations."""
    total = df.count()
    if total == 0:
        raise ValueError(f"Quality check failed for '{label}': DataFrame is empty")

    null_event_ids = df.filter(F.col("event_id").isNull()).count()
    null_user_ids = df.filter(F.col("user_id").isNull()).count()
    null_timestamps = df.filter(F.col("event_ts").isNull()).count()

    violations = {
        "null_event_id": null_event_ids,
        "null_user_id": null_user_ids,
        "null_event_ts": null_timestamps,
    }
    failed = {k: v for k, v in violations.items() if v > 0}
    if failed:
        pct = {k: round(v / total * 100, 2) for k, v in failed.items()}
        logger.warning("Quality violations in '%s': %s (pct of total: %s)", label, failed, pct)
        # Fail hard if more than 5 % of records have null PKs
        for key in ("null_event_id", "null_user_id"):
            if violations[key] / total > 0.05:
                raise ValueError(
                    f"Quality gate FAILED: {key} rate {violations[key]/total:.2%} exceeds 5% threshold"
                )
    else:
        logger.info("Quality check '%s' passed — %d records OK", label, total)


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


def write_output(df: DataFrame, output_path: str, output_format: str) -> int:
    """Write the transformed DataFrame partitioned by event_date."""
    row_count = df.count()
    logger.info("Writing %d transformed records to %s", row_count, output_path)

    df_out = df.withColumn("_transformed_at", F.current_timestamp())

    # Repartition for optimal file sizes (~128 MB target)
    df_out = df_out.repartition(OUTPUT_PARTITIONS, "event_date")

    writer = df_out.write.mode("overwrite").partitionBy("event_date")
    if output_format == "delta":
        writer.format("delta").save(output_path)
    else:
        writer.format("parquet").option("compression", "snappy").save(output_path)

    logger.info("Write complete — %d records, format=%s", row_count, output_format)
    return row_count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform raw event data.")
    parser.add_argument("--input-path", required=True, help="Path to raw events.")
    parser.add_argument("--output-path", required=True, help="Path for transformed output.")
    parser.add_argument(
        "--format",
        dest="output_format",
        choices=["delta", "parquet"],
        default="delta",
    )
    parser.add_argument("--batch-date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    parser.add_argument("--input-format", choices=["delta", "parquet"], default="delta")
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)

    logger.info(
        "Starting event transformation — input=%s output=%s date=%s",
        args.input_path,
        args.output_path,
        args.batch_date,
    )

    spark = (
        SparkSession.builder.appName("transform_events")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Register UDFs for SQL usage as well
    spark.udf.register("anonymise_ip", _anonymise_ip, StringType())
    spark.udf.register("classify_device", _classify_device, StringType())

    try:
        # 1. Read
        raw = read_source(spark, args.input_path, args.input_format)

        # 2. Deduplicate
        deduped = deduplicate(raw)

        # 3. Parse timestamp & derive date
        parsed = (
            deduped.withColumn("event_ts", F.to_timestamp("event_timestamp"))
            .withColumn("event_date", F.to_date("event_ts"))
            .withColumn("event_hour", F.hour("event_ts"))
        )

        # 4. Enrich with UDFs
        enriched = enrich_with_udfs(spark, parsed)

        # Cache — we reuse this DataFrame multiple times
        enriched.cache()
        logger.info("Cached enriched DataFrame (%d records)", enriched.count())

        # 5. Window functions
        windowed = add_window_functions(enriched)

        # 6. Sessionize
        sessionized = sessionize(windowed)

        # 7. Session aggregates (broadcast join)
        with_session_stats = compute_session_aggregates(sessionized)

        # 8. Quality assertions
        assert_quality(with_session_stats, "post_transform")

        # 9. Write main output
        final_columns = [
            "event_id",
            "user_id",
            "event_type",
            "event_ts",
            "event_date",
            "event_hour",
            "platform",
            "app_version",
            "ip_hash",
            "device_category",
            "prev_event_type",
            "next_event_type",
            "event_rank_per_user",
            "seconds_since_last_event",
            "computed_session_id",
            "session_event_count",
            "session_start",
            "session_end",
            "session_duration_seconds",
            "session_distinct_event_types",
            "session_entry_event",
            "session_exit_event",
        ]
        output_df = with_session_stats.select(
            [c for c in final_columns if c in with_session_stats.columns]
        )
        rows = write_output(output_df, args.output_path, args.output_format)

        # 10. Optionally write pivot summary
        pivot_path = args.output_path.rstrip("/") + "_pivot_summary"
        pivot_df = pivot_event_counts(enriched)
        pivot_df.write.mode("overwrite").format("parquet").save(pivot_path)
        logger.info("Pivot summary written to %s", pivot_path)

        # Unpersist
        enriched.unpersist()

        logger.info("Transformation pipeline complete — %d rows", rows)

    except Exception:
        logger.exception("Fatal error during transformation")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
