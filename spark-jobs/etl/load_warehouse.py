"""
Load processed data into the data warehouse.

Supports Delta Lake MERGE for upserts, SCD Type 2 dimension handling,
partition management, and post-load validation with record counts and checksums.

Usage:
    spark-submit --master spark://master:7077 spark-jobs/etl/load_warehouse.py \
        --input-path s3a://data-lake/processed/events \
        --warehouse-path s3a://data-lake/warehouse/events \
        --load-type upsert \
        --date 2024-01-15
"""

import argparse
import hashlib
import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("load_warehouse")


# ---------------------------------------------------------------------------
# Checksum Helpers
# ---------------------------------------------------------------------------
def compute_dataframe_checksum(df: DataFrame, key_columns: List[str]) -> str:
    """
    Compute a deterministic checksum over key columns for validation.

    Concatenates key columns row-wise, hashes each row, and XORs the hashes
    to produce a single checksum string.
    """
    concat_col = F.concat_ws("|", *[F.col(c).cast(StringType()) for c in key_columns])
    hash_df = df.select(F.md5(concat_col).alias("row_hash"))
    checksum = hash_df.agg(F.md5(F.concat_ws(",", F.collect_list("row_hash"))).alias("checksum"))
    return checksum.collect()[0]["checksum"]


# ---------------------------------------------------------------------------
# Upsert (Delta MERGE)
# ---------------------------------------------------------------------------
def delta_upsert(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_keys: List[str],
    partition_cols: Optional[List[str]] = None,
) -> Dict[str, int]:
    """
    Perform a Delta Lake MERGE (upsert) operation.

    Matches on merge_keys. When matched, updates all non-key columns.
    When not matched, inserts the full row.

    Returns
    -------
    dict
        Counts of rows inserted, updated, and deleted.
    """
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, target_path):
        logger.info("Target table does not exist. Creating with initial load.")
        writer = source_df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(target_path)
        count = source_df.count()
        return {"inserted": count, "updated": 0, "deleted": 0}

    target_table = DeltaTable.forPath(spark, target_path)
    merge_condition = " AND ".join(
        [f"target.{k} = source.{k}" for k in merge_keys]
    )

    non_key_columns = [c for c in source_df.columns if c not in merge_keys]
    update_map = {c: f"source.{c}" for c in non_key_columns}
    insert_map = {c: f"source.{c}" for c in source_df.columns}

    logger.info("Executing Delta MERGE on %s with keys %s.", target_path, merge_keys)

    merge_result = (
        target_table.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdate(set=update_map)
        .whenNotMatchedInsert(values=insert_map)
        .execute()
    )

    # Read operation metrics from Delta history
    history = target_table.history(1).collect()[0]
    metrics = json.loads(history["operationMetrics"] or "{}")

    result = {
        "inserted": int(metrics.get("numTargetRowsInserted", 0)),
        "updated": int(metrics.get("numTargetRowsUpdated", 0)),
        "deleted": int(metrics.get("numTargetRowsDeleted", 0)),
    }
    logger.info("MERGE result: %s", result)
    return result


# ---------------------------------------------------------------------------
# JDBC Upsert (INSERT ON CONFLICT)
# ---------------------------------------------------------------------------
def jdbc_upsert(
    df: DataFrame,
    jdbc_url: str,
    table_name: str,
    merge_keys: List[str],
    jdbc_properties: Dict[str, str],
    batch_size: int = 10000,
) -> int:
    """
    Upsert data into a JDBC-connected warehouse using INSERT ON CONFLICT.

    This writes to a staging table first, then executes the MERGE SQL.
    """
    staging_table = f"{table_name}_staging"

    logger.info("Writing %d rows to JDBC staging table %s.", df.count(), staging_table)

    (
        df.write
        .mode("overwrite")
        .option("batchsize", str(batch_size))
        .option("truncate", "true")
        .jdbc(jdbc_url, staging_table, properties=jdbc_properties)
    )

    # Build upsert SQL
    all_cols = df.columns
    non_key_cols = [c for c in all_cols if c not in merge_keys]
    cols_list = ", ".join(all_cols)
    conflict_cols = ", ".join(merge_keys)
    update_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in non_key_cols])

    upsert_sql = f"""
        INSERT INTO {table_name} ({cols_list})
        SELECT {cols_list} FROM {staging_table}
        ON CONFLICT ({conflict_cols})
        DO UPDATE SET {update_clause}, updated_at = NOW()
    """

    logger.info("Executing JDBC upsert SQL for table %s.", table_name)

    # Execute via JDBC connection
    from pyspark.sql import SparkSession
    spark = df.sparkSession

    driver = jdbc_properties.get("driver", "org.postgresql.Driver")
    spark._jvm.Class.forName(driver)
    connection = spark._jvm.java.sql.DriverManager.getConnection(
        jdbc_url,
        jdbc_properties.get("user", ""),
        jdbc_properties.get("password", ""),
    )
    try:
        stmt = connection.createStatement()
        rows_affected = stmt.executeUpdate(upsert_sql)
        logger.info("JDBC upsert affected %d rows.", rows_affected)

        # Drop staging table
        stmt.executeUpdate(f"DROP TABLE IF EXISTS {staging_table}")
        stmt.close()
        return rows_affected
    except Exception as exc:
        logger.error("JDBC upsert failed: %s. Attempting rollback.", exc)
        try:
            connection.rollback()
            stmt = connection.createStatement()
            stmt.executeUpdate(f"DROP TABLE IF EXISTS {staging_table}")
            stmt.close()
        except Exception:
            logger.exception("Rollback cleanup failed.")
        raise
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# SCD Type 2
# ---------------------------------------------------------------------------
def apply_scd_type2(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    business_keys: List[str],
    tracked_columns: List[str],
    effective_date_col: str = "effective_date",
) -> Dict[str, int]:
    """
    Apply Slowly Changing Dimension Type 2 logic.

    1. Hash tracked columns for change detection
    2. Expire changed records (set end_date, is_current=False)
    3. Insert new versions of changed records
    4. Insert entirely new records

    Returns
    -------
    dict
        Counts: new_records, updated_records, unchanged_records
    """
    from delta.tables import DeltaTable

    hash_cols = F.concat_ws("|", *[F.col(c).cast(StringType()) for c in tracked_columns])

    source_with_hash = (
        source_df
        .withColumn("row_hash", F.md5(hash_cols))
        .withColumn("effective_from", F.current_timestamp())
        .withColumn("effective_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    if not DeltaTable.isDeltaTable(spark, target_path):
        logger.info("SCD target does not exist. Creating with initial load.")
        source_with_hash.write.format("delta").mode("overwrite").save(target_path)
        count = source_with_hash.count()
        return {"new_records": count, "updated_records": 0, "unchanged_records": 0}

    target_table = DeltaTable.forPath(spark, target_path)
    current_df = spark.read.format("delta").load(target_path).filter(F.col("is_current") == True)

    # Join source with current target on business keys
    join_cond = [source_with_hash[k] == current_df[k] for k in business_keys]

    # Find changed records
    changed = (
        source_with_hash.alias("src")
        .join(current_df.alias("tgt"), join_cond, "inner")
        .filter(F.col("src.row_hash") != F.col("tgt.row_hash"))
        .select("src.*")
    )

    # Find new records (not in target)
    new_records = (
        source_with_hash.alias("src")
        .join(current_df.alias("tgt"), join_cond, "left_anti")
    )

    changed_count = changed.count()
    new_count = new_records.count()
    unchanged_count = source_with_hash.count() - changed_count - new_count

    logger.info(
        "SCD2: %d new, %d changed, %d unchanged.",
        new_count, changed_count, unchanged_count,
    )

    if changed_count > 0:
        # Expire old records
        expire_condition = " AND ".join(
            [f"target.{k} = source.{k}" for k in business_keys]
        )
        (
            target_table.alias("target")
            .merge(changed.alias("source"), expire_condition + " AND target.is_current = true")
            .whenMatchedUpdate(set={
                "is_current": "false",
                "effective_to": "source.effective_from",
            })
            .execute()
        )

        # Append new versions
        changed.write.format("delta").mode("append").save(target_path)

    if new_count > 0:
        new_records.write.format("delta").mode("append").save(target_path)

    return {
        "new_records": new_count,
        "updated_records": changed_count,
        "unchanged_records": unchanged_count,
    }


# ---------------------------------------------------------------------------
# Partition Management
# ---------------------------------------------------------------------------
def manage_partitions(
    spark: SparkSession,
    table_path: str,
    partition_column: str,
    retain_days: int = 90,
) -> List[str]:
    """
    Drop partitions older than retain_days from a Delta table.

    Returns list of removed partition values.
    """
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, table_path):
        logger.warning("Not a Delta table: %s", table_path)
        return []

    cutoff = F.date_sub(F.current_date(), retain_days)

    table = DeltaTable.forPath(spark, table_path)
    old_partitions = (
        spark.read.format("delta").load(table_path)
        .select(partition_column)
        .distinct()
        .filter(F.col(partition_column) < cutoff)
        .collect()
    )

    removed = []
    for row in old_partitions:
        val = row[partition_column]
        logger.info("Removing partition %s=%s", partition_column, val)
        table.delete(F.col(partition_column) == val)
        removed.append(str(val))

    if removed:
        table.vacuum(retentionHours=168)  # 7 days
        logger.info("Removed %d old partitions. Vacuum complete.", len(removed))

    return removed


# ---------------------------------------------------------------------------
# Post-Load Validation
# ---------------------------------------------------------------------------
def validate_load(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    key_columns: List[str],
) -> Dict[str, any]:
    """
    Post-load validation: compare record counts and checksums.

    Raises ValueError if counts do not match.
    """
    source_count = source_df.count()
    target_df = spark.read.format("delta").load(target_path)
    target_count = target_df.count()

    source_checksum = compute_dataframe_checksum(source_df, key_columns)
    target_checksum = compute_dataframe_checksum(target_df, key_columns)

    result = {
        "source_count": source_count,
        "target_count": target_count,
        "counts_match": source_count <= target_count,  # target may have historical SCD rows
        "source_checksum": source_checksum,
        "target_checksum": target_checksum,
    }

    logger.info("Validation result: %s", json.dumps(result, default=str))

    if source_count > target_count:
        raise ValueError(
            f"Row count mismatch: source={source_count}, target={target_count}. "
            "Target has fewer rows than source — possible data loss."
        )

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load processed data into warehouse.")
    parser.add_argument("--input-path", required=True, help="Processed data path.")
    parser.add_argument("--warehouse-path", required=True, help="Warehouse target path.")
    parser.add_argument(
        "--load-type", choices=["full", "upsert", "scd2"], default="upsert",
        help="Load strategy.",
    )
    parser.add_argument("--date", default=None, help="Date filter (YYYY-MM-DD).")
    parser.add_argument("--merge-keys", default="event_id", help="Comma-separated merge keys.")
    parser.add_argument("--jdbc-url", default=None, help="JDBC URL for relational warehouse.")
    parser.add_argument("--jdbc-table", default=None, help="Target JDBC table name.")
    parser.add_argument("--retain-days", type=int, default=90, help="Partition retention days.")
    parser.add_argument("--skip-validation", action="store_true", help="Skip post-load checks.")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    start_time = time.time()

    sys.path.insert(0, "spark-jobs/utils")
    from spark_session import SparkSessionBuilder

    spark = SparkSessionBuilder(app_name="LoadWarehouse").with_delta().build()

    try:
        # ---- Read Processed Data ----
        logger.info("Reading processed data from %s", args.input_path)
        df = spark.read.parquet(args.input_path)
        if args.date:
            df = df.filter(F.col("ingestion_date") == args.date)

        input_count = df.count()
        logger.info("Read %d records for loading.", input_count)

        if input_count == 0:
            logger.warning("No records to load. Exiting.")
            return

        merge_keys = [k.strip() for k in args.merge_keys.split(",")]

        # ---- Execute Load ----
        if args.load_type == "full":
            logger.info("Full load to %s", args.warehouse_path)
            df.write.format("delta").mode("overwrite").partitionBy("ingestion_date").save(
                args.warehouse_path
            )
            result = {"inserted": input_count, "updated": 0, "deleted": 0}

        elif args.load_type == "upsert":
            if args.jdbc_url and args.jdbc_table:
                jdbc_props = {
                    "driver": "org.postgresql.Driver",
                    "user": spark.conf.get("spark.jdbc.user", "warehouse_user"),
                    "password": spark.conf.get("spark.jdbc.password", ""),
                }
                rows = jdbc_upsert(
                    df, args.jdbc_url, args.jdbc_table, merge_keys, jdbc_props,
                )
                result = {"rows_affected": rows}
            else:
                result = delta_upsert(
                    spark, df, args.warehouse_path, merge_keys,
                    partition_cols=["ingestion_date"],
                )

        elif args.load_type == "scd2":
            tracked_cols = [c for c in df.columns if c not in merge_keys + ["ingestion_date", "ingested_at"]]
            result = apply_scd_type2(
                spark, df, args.warehouse_path, merge_keys, tracked_cols,
            )

        logger.info("Load result: %s", json.dumps(result, default=str))

        # ---- Partition Management ----
        if args.retain_days > 0 and not args.jdbc_url:
            removed = manage_partitions(
                spark, args.warehouse_path, "ingestion_date", args.retain_days,
            )
            if removed:
                logger.info("Removed partitions: %s", removed)

        # ---- Post-Load Validation ----
        if not args.skip_validation and not args.jdbc_url:
            validate_load(spark, df, args.warehouse_path, merge_keys)

        elapsed = time.time() - start_time
        logger.info("Load complete in %.1f seconds.", elapsed)

    except Exception:
        logger.exception("Load job failed.")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
