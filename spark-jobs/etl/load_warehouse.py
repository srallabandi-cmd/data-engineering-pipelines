"""
Warehouse Loading Module
========================

Load transformed data into Delta Lake with MERGE (upsert) semantics,
table optimisation (OPTIMIZE, VACUUM, ZORDER), schema evolution handling,
idempotent writes, and write-metrics logging.

Usage:
    spark-submit --packages io.delta:delta-core_2.12:2.4.0 \
        spark-jobs/etl/load_warehouse.py \
        --input-path s3a://data-lake/transformed/events \
        --target-path s3a://data-warehouse/events \
        --batch-date 2024-01-15
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("load_warehouse")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MERGE_KEY_COLUMNS = ["event_id"]
PARTITION_COLUMNS = ["event_date"]
ZORDER_COLUMNS = ["user_id", "event_type"]
VACUUM_RETENTION_HOURS = 168  # 7 days


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def build_merge_condition(keys: List[str]) -> str:
    """Build a SQL merge condition from a list of key columns."""
    return " AND ".join(f"target.{k} = source.{k}" for k in keys)


def log_write_metrics(
    operation: str,
    rows_affected: int,
    duration_seconds: float,
    batch_date: str,
    extra: Optional[Dict] = None,
) -> None:
    """Log structured write metrics for observability."""
    metrics = {
        "operation": operation,
        "rows_affected": rows_affected,
        "duration_seconds": round(duration_seconds, 2),
        "batch_date": batch_date,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if extra:
        metrics.update(extra)
    logger.info("WRITE_METRICS | %s", json.dumps(metrics))


# ---------------------------------------------------------------------------
# Core operations
# ---------------------------------------------------------------------------


def read_transformed(
    spark: SparkSession, input_path: str, input_format: str
) -> DataFrame:
    """Read transformed data from the staging area."""
    logger.info("Reading transformed data from %s (%s)", input_path, input_format)
    if input_format == "delta":
        df = spark.read.format("delta").load(input_path)
    else:
        df = spark.read.format("parquet").load(input_path)

    row_count = df.count()
    logger.info("Read %d records from transformed layer", row_count)
    return df


def upsert_to_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_keys: List[str],
    batch_date: str,
) -> int:
    """Perform an idempotent MERGE (upsert) into the target Delta table.

    - Matched rows are updated.
    - Unmatched rows are inserted.
    - The operation is idempotent: re-running with the same batch_date
      produces the same result.
    """
    start = time.time()

    # Add load metadata
    source = source_df.withColumn(
        "_loaded_at", F.current_timestamp()
    ).withColumn("_batch_date", F.lit(batch_date))

    source_count = source.count()

    # Check if target exists
    try:
        target_table = DeltaTable.forPath(spark, target_path)
        logger.info("Target table exists at %s — performing MERGE", target_path)

        merge_condition = build_merge_condition(merge_keys)

        # Build the update set: all columns from source
        update_set = {c: f"source.{c}" for c in source.columns}
        insert_set = {c: f"source.{c}" for c in source.columns}

        (
            target_table.alias("target")
            .merge(source.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_set)
            .execute()
        )

        duration = time.time() - start
        log_write_metrics("MERGE", source_count, duration, batch_date)
        return source_count

    except Exception as e:
        if "is not a Delta table" in str(e) or "doesn't exist" in str(e) or "Path does not exist" in str(e):
            logger.info("Target does not exist — performing initial WRITE")
            source.write.format("delta").partitionBy(*PARTITION_COLUMNS).mode(
                "overwrite"
            ).option("overwriteSchema", "true").save(target_path)

            duration = time.time() - start
            log_write_metrics("INITIAL_WRITE", source_count, duration, batch_date)
            return source_count
        raise


def write_to_parquet(
    df: DataFrame, output_path: str, partition_cols: List[str], batch_date: str
) -> int:
    """Write a Parquet copy of the data for systems that do not read Delta."""
    start = time.time()
    row_count = df.count()

    df_out = df.withColumn("_loaded_at", F.current_timestamp()).withColumn(
        "_batch_date", F.lit(batch_date)
    )

    (
        df_out.write.mode("overwrite")
        .partitionBy(*partition_cols)
        .option("compression", "snappy")
        .format("parquet")
        .save(output_path)
    )

    duration = time.time() - start
    log_write_metrics("PARQUET_WRITE", row_count, duration, batch_date)
    return row_count


def handle_schema_evolution(spark: SparkSession, target_path: str) -> None:
    """Enable automatic schema evolution on the target Delta table."""
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    logger.info("Schema auto-merge enabled for target at %s", target_path)


def optimize_table(spark: SparkSession, target_path: str) -> None:
    """Run Delta OPTIMIZE with ZORDER to compact small files and co-locate data."""
    start = time.time()
    try:
        delta_table = DeltaTable.forPath(spark, target_path)

        # Compact small files
        logger.info("Running OPTIMIZE on %s", target_path)
        spark.sql(f"OPTIMIZE delta.`{target_path}`")

        # Z-Order by frequently queried columns
        zorder_cols = ", ".join(ZORDER_COLUMNS)
        logger.info("Running ZORDER BY (%s) on %s", zorder_cols, target_path)
        spark.sql(f"OPTIMIZE delta.`{target_path}` ZORDER BY ({zorder_cols})")

        duration = time.time() - start
        logger.info("OPTIMIZE + ZORDER complete in %.1fs", duration)

    except Exception:
        logger.warning("OPTIMIZE/ZORDER failed — table may not support it", exc_info=True)


def vacuum_table(spark: SparkSession, target_path: str) -> None:
    """Remove old files beyond the retention period."""
    start = time.time()
    try:
        # Disable the safety check so we can vacuum at custom retention
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

        logger.info(
            "Running VACUUM on %s (retention = %d hours)",
            target_path,
            VACUUM_RETENTION_HOURS,
        )
        spark.sql(f"VACUUM delta.`{target_path}` RETAIN {VACUUM_RETENTION_HOURS} HOURS")

        duration = time.time() - start
        logger.info("VACUUM complete in %.1fs", duration)

    except Exception:
        logger.warning("VACUUM failed", exc_info=True)


def get_table_stats(spark: SparkSession, target_path: str) -> Dict:
    """Return basic statistics about the target Delta table."""
    try:
        dt = DeltaTable.forPath(spark, target_path)
        history = dt.history(1).collect()
        detail = spark.sql(f"DESCRIBE DETAIL delta.`{target_path}`").collect()

        stats = {
            "last_operation": history[0]["operation"] if history else "unknown",
            "last_operation_timestamp": str(
                history[0]["timestamp"] if history else "unknown"
            ),
            "num_files": detail[0]["numFiles"] if detail else -1,
            "size_bytes": detail[0]["sizeInBytes"] if detail else -1,
        }
        logger.info("Table stats: %s", json.dumps(stats))
        return stats
    except Exception:
        logger.warning("Could not retrieve table stats", exc_info=True)
        return {}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load transformed data into warehouse.")
    parser.add_argument("--input-path", required=True, help="Path to transformed data.")
    parser.add_argument("--target-path", required=True, help="Delta Lake target path.")
    parser.add_argument(
        "--parquet-path",
        default=None,
        help="Optional Parquet mirror path.",
    )
    parser.add_argument(
        "--input-format",
        choices=["delta", "parquet"],
        default="delta",
    )
    parser.add_argument(
        "--batch-date",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    )
    parser.add_argument(
        "--skip-optimize",
        action="store_true",
        help="Skip OPTIMIZE and VACUUM steps.",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)

    logger.info(
        "Starting warehouse load — input=%s target=%s date=%s",
        args.input_path,
        args.target_path,
        args.batch_date,
    )

    spark = (
        SparkSession.builder.appName("load_warehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Read transformed data
        source_df = read_transformed(spark, args.input_path, args.input_format)

        # 2. Schema evolution
        handle_schema_evolution(spark, args.target_path)

        # 3. MERGE / upsert
        rows_merged = upsert_to_delta(
            spark, source_df, args.target_path, MERGE_KEY_COLUMNS, args.batch_date
        )

        # 4. Optional Parquet mirror
        if args.parquet_path:
            write_to_parquet(
                source_df, args.parquet_path, PARTITION_COLUMNS, args.batch_date
            )

        # 5. Optimise
        if not args.skip_optimize:
            optimize_table(spark, args.target_path)
            vacuum_table(spark, args.target_path)

        # 6. Table stats
        get_table_stats(spark, args.target_path)

        logger.info("Warehouse load complete — %d rows merged", rows_merged)

    except Exception:
        logger.exception("Fatal error during warehouse load")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
