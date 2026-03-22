"""
Transform raw events from the staging layer into enriched, analytics-ready datasets.

This module applies complex PySpark transformations including window functions,
session identification, pivot operations, UDFs, data enrichment joins,
and partition optimization before writing to the processed layer.

Usage:
    spark-submit --master spark://master:7077 spark-jobs/etl/transform_events.py \
        --input-path s3a://data-lake/staging/events \
        --output-path s3a://data-lake/processed/events \
        --dimensions-path s3a://data-lake/dimensions \
        --date 2024-01-15
"""

import argparse
import hashlib
import json
import logging
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, TimestampType, IntegerType, MapType,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("transform_events")


# ---------------------------------------------------------------------------
# UDF Definitions
# ---------------------------------------------------------------------------
@F.udf(StringType())
def classify_device(device_type: str, os: str) -> str:
    """Classify device into a normalized category."""
    if device_type is None:
        return "unknown"
    device_lower = device_type.lower()
    if device_lower in ("mobile", "phone", "smartphone"):
        return "mobile"
    elif device_lower in ("tablet", "ipad"):
        return "tablet"
    elif device_lower in ("desktop", "laptop", "pc"):
        return "desktop"
    elif device_lower in ("tv", "smart_tv", "console"):
        return "smart_tv"
    elif device_lower == "bot" or (os and os.lower() == "bot"):
        return "bot"
    return "other"


@F.udf(StringType())
def extract_utm_source(page_url: str) -> str:
    """Extract UTM source parameter from a URL."""
    if page_url is None:
        return None
    try:
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(page_url)
        params = parse_qs(parsed.query)
        sources = params.get("utm_source", [None])
        return sources[0]
    except Exception:
        return None


@F.udf(StringType())
def hash_pii(value: str) -> str:
    """One-way hash for PII fields."""
    if value is None:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Transformation Functions
# ---------------------------------------------------------------------------
SESSION_TIMEOUT_SECONDS = 1800  # 30 minutes


def add_session_boundaries(df: DataFrame) -> DataFrame:
    """
    Identify user sessions based on a 30-minute inactivity timeout.

    Uses window functions (lag) to compute time gaps between consecutive
    events per user, then assigns a session identifier.
    """
    user_window = Window.partitionBy("user_id").orderBy("event_timestamp")

    df_with_lag = df.withColumn(
        "prev_event_ts", F.lag("event_timestamp").over(user_window)
    ).withColumn(
        "time_gap_seconds",
        F.when(
            F.col("prev_event_ts").isNotNull(),
            F.unix_timestamp("event_timestamp") - F.unix_timestamp("prev_event_ts"),
        ).otherwise(F.lit(SESSION_TIMEOUT_SECONDS + 1)),
    )

    df_with_boundary = df_with_lag.withColumn(
        "is_new_session",
        F.when(F.col("time_gap_seconds") > SESSION_TIMEOUT_SECONDS, F.lit(1)).otherwise(F.lit(0)),
    )

    df_with_session_idx = df_with_boundary.withColumn(
        "session_idx",
        F.sum("is_new_session").over(
            user_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ),
    )

    df_with_session = df_with_session_idx.withColumn(
        "computed_session_id",
        F.concat_ws("_", F.col("user_id"), F.col("session_idx").cast(StringType())),
    )

    return df_with_session.drop("prev_event_ts", "time_gap_seconds", "is_new_session", "session_idx")


def add_event_sequence_features(df: DataFrame) -> DataFrame:
    """
    Add sequencing features with window functions.

    - row_number, dense_rank within each session
    - lead/lag of event_type (previous_event, next_event)
    - time to next event
    - running event count
    """
    session_window = Window.partitionBy("user_id", "computed_session_id").orderBy("event_timestamp")

    return (
        df
        .withColumn("event_sequence_number", F.row_number().over(session_window))
        .withColumn("event_rank", F.dense_rank().over(session_window))
        .withColumn("previous_event_type", F.lag("event_type", 1).over(session_window))
        .withColumn("next_event_type", F.lead("event_type", 1).over(session_window))
        .withColumn("next_event_ts", F.lead("event_timestamp", 1).over(session_window))
        .withColumn(
            "seconds_to_next_event",
            F.when(
                F.col("next_event_ts").isNotNull(),
                F.unix_timestamp("next_event_ts") - F.unix_timestamp("event_timestamp"),
            ),
        )
        .withColumn(
            "cumulative_duration_ms",
            F.sum("duration_ms").over(
                session_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        .drop("next_event_ts")
    )


def compute_session_aggregates(df: DataFrame) -> DataFrame:
    """
    Compute per-session aggregate metrics.

    Groups by user_id and computed_session_id with multiple agg functions,
    then joins back to the event-level data.
    """
    session_aggs = (
        df
        .groupBy("user_id", "computed_session_id")
        .agg(
            F.count("event_id").alias("session_event_count"),
            F.min("event_timestamp").alias("session_start"),
            F.max("event_timestamp").alias("session_end"),
            F.sum("duration_ms").alias("session_total_duration_ms"),
            F.sum("revenue").alias("session_total_revenue"),
            F.countDistinct("event_type").alias("session_distinct_events"),
            F.max("is_conversion").alias("session_has_conversion"),
            F.first("device_type").alias("session_device"),
            F.first("country").alias("session_country"),
        )
        .withColumn(
            "session_duration_seconds",
            F.unix_timestamp("session_end") - F.unix_timestamp("session_start"),
        )
    )
    return session_aggs


def compute_event_type_pivot(df: DataFrame) -> DataFrame:
    """
    Pivot event counts by event_type per user per day.

    Produces a wide table with one column per event type.
    """
    return (
        df
        .withColumn("event_date", F.to_date("event_timestamp"))
        .groupBy("user_id", "event_date")
        .pivot(
            "event_type",
            values=["page_view", "click", "scroll", "purchase", "signup", "login", "logout"],
        )
        .agg(F.count("event_id"))
        .na.fill(0)
    )


def apply_device_classification(df: DataFrame) -> DataFrame:
    """Apply the device classification UDF and UTM extraction."""
    return (
        df
        .withColumn("device_category", classify_device(F.col("device_type"), F.col("os")))
        .withColumn("utm_source", extract_utm_source(F.col("page_url")))
    )


def enrich_with_dimensions(
    df: DataFrame,
    dim_users: DataFrame,
    dim_geo: Optional[DataFrame] = None,
) -> DataFrame:
    """
    Join event data with dimension tables for enrichment.

    Performs left joins to preserve all events even if dimension data is missing.
    Uses broadcast hints for small dimensions.
    """
    enriched = df.join(
        F.broadcast(dim_users.select(
            F.col("user_id"),
            F.col("user_segment"),
            F.col("signup_date").alias("user_signup_date"),
            F.col("lifetime_value").alias("user_ltv"),
            F.col("is_active").alias("user_is_active"),
        )),
        on="user_id",
        how="left",
    )

    if dim_geo is not None:
        enriched = enriched.join(
            F.broadcast(dim_geo.select(
                F.col("country"),
                F.col("region"),
                F.col("continent"),
                F.col("timezone"),
            )),
            on="country",
            how="left",
        )

    enriched = enriched.withColumn(
        "days_since_signup",
        F.when(
            F.col("user_signup_date").isNotNull(),
            F.datediff(F.col("event_timestamp"), F.col("user_signup_date")),
        ),
    )

    return enriched


def optimize_and_write(
    df: DataFrame,
    output_path: str,
    partition_cols: List[str],
    target_partitions: int = 200,
    file_format: str = "parquet",
) -> int:
    """
    Repartition and write the transformed DataFrame.

    Uses coalesce when reducing partitions for efficiency and repartition
    when a shuffle is required for balanced partition sizes.
    """
    record_count = df.count()
    logger.info("Preparing to write %d records.", record_count)

    if record_count == 0:
        logger.warning("No records to write.")
        return 0

    # Determine optimal number of output files
    # Aim for ~128 MB per file assuming average row size of 1 KB
    estimated_size_mb = (record_count * 1024) / (1024 * 1024)
    optimal_files = max(1, int(estimated_size_mb / 128))
    optimal_files = min(optimal_files, target_partitions)

    logger.info(
        "Repartitioning to %d files (estimated size: %.0f MB).",
        optimal_files, estimated_size_mb,
    )

    repartitioned = df.repartition(optimal_files, *[F.col(c) for c in partition_cols])

    writer = repartitioned.write.mode("overwrite").partitionBy(*partition_cols)

    if file_format == "delta":
        writer.format("delta").option("overwriteSchema", "true").save(output_path)
    else:
        writer.option("compression", "snappy").parquet(output_path)

    logger.info("Write complete: %d records to %s.", record_count, output_path)
    return record_count


# ---------------------------------------------------------------------------
# Output Schema Validation
# ---------------------------------------------------------------------------
EXPECTED_OUTPUT_COLUMNS = {
    "event_id", "event_type", "user_id", "computed_session_id",
    "event_timestamp", "device_category", "utm_source",
    "event_sequence_number", "previous_event_type", "next_event_type",
    "seconds_to_next_event", "cumulative_duration_ms",
    "ingestion_date",
}


def validate_output_schema(df: DataFrame) -> None:
    """Validate the output DataFrame contains all expected columns."""
    actual_columns = set(df.columns)
    missing = EXPECTED_OUTPUT_COLUMNS - actual_columns
    if missing:
        raise ValueError(f"Output DataFrame missing columns: {missing}")
    logger.info("Output schema validation passed (%d columns).", len(actual_columns))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform events from staging to processed.")
    parser.add_argument("--input-path", required=True, help="Staging events path.")
    parser.add_argument("--output-path", required=True, help="Processed events output path.")
    parser.add_argument("--dimensions-path", required=True, help="Dimensions base path.")
    parser.add_argument("--date", default=None, help="Processing date (YYYY-MM-DD).")
    parser.add_argument("--format", choices=["parquet", "delta"], default="parquet")
    parser.add_argument("--target-partitions", type=int, default=200)
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    start_time = time.time()

    sys.path.insert(0, "spark-jobs/utils")
    from spark_session import SparkSessionBuilder

    spark = SparkSessionBuilder(app_name="TransformEvents").with_delta().build()

    try:
        # ---- Read Staging Data ----
        logger.info("Reading staging data from %s", args.input_path)
        if args.date:
            events_df = spark.read.parquet(args.input_path).filter(
                F.col("ingestion_date") == args.date
            )
        else:
            events_df = spark.read.parquet(args.input_path)

        input_count = events_df.count()
        logger.info("Read %d events from staging.", input_count)

        if input_count == 0:
            logger.warning("No events to process. Exiting.")
            return

        # ---- Read Dimension Tables ----
        dim_users_path = f"{args.dimensions_path}/dim_users"
        logger.info("Reading dimension: %s", dim_users_path)
        dim_users = spark.read.parquet(dim_users_path)

        # ---- Apply Transformations ----
        logger.info("Applying device classification and UTM extraction.")
        df = apply_device_classification(events_df)

        logger.info("Computing session boundaries.")
        df = add_session_boundaries(df)

        logger.info("Adding event sequence features.")
        df = add_event_sequence_features(df)

        logger.info("Enriching with dimension tables.")
        df = enrich_with_dimensions(df, dim_users)

        # ---- Validate ----
        validate_output_schema(df)

        # ---- Write Processed Data ----
        partition_cols = ["ingestion_date", "event_type"]
        records_written = optimize_and_write(
            df, args.output_path, partition_cols,
            target_partitions=args.target_partitions,
            file_format=args.format,
        )

        # ---- Write Session Aggregates (side output) ----
        session_aggs = compute_session_aggregates(df)
        session_output = args.output_path.rstrip("/") + "_sessions"
        optimize_and_write(
            session_aggs, session_output,
            partition_cols=["session_country"],
            target_partitions=50,
            file_format=args.format,
        )

        # ---- Write Pivot Table (side output) ----
        pivot_df = compute_event_type_pivot(df)
        pivot_output = args.output_path.rstrip("/") + "_pivots"
        optimize_and_write(
            pivot_df, pivot_output,
            partition_cols=["event_date"],
            target_partitions=10,
            file_format=args.format,
        )

        elapsed = time.time() - start_time
        logger.info(
            "Transformation complete. Input: %d, Output: %d, Duration: %.1fs.",
            input_count, records_written, elapsed,
        )

    except Exception:
        logger.exception("Transformation job failed.")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
