"""
API Data Extraction Module
==========================

Production-grade data extraction from REST APIs with pagination, rate limiting,
exponential backoff retries, schema enforcement, and Delta Lake output.

Usage:
    spark-submit --packages io.delta:delta-core_2.12:2.4.0 \
        spark-jobs/etl/extract_api_data.py \
        --api-url https://api.example.com/v1/events \
        --output-path s3a://data-lake/raw/events \
        --format delta \
        --batch-date 2024-01-15
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional, Tuple

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("extract_api_data")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_PAGE_SIZE = 500
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 60.0
BACKOFF_FACTOR = 2.0
RATE_LIMIT_REQUESTS_PER_SECOND = 10
DEAD_LETTER_SUFFIX = "_dead_letter"

# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------
EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_timestamp", StringType(), nullable=False),
        StructField("event_properties", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("platform", StringType(), nullable=True),
        StructField("app_version", StringType(), nullable=True),
        StructField("ip_address", StringType(), nullable=True),
        StructField("user_agent", StringType(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------
class RateLimiter:
    """Token-bucket rate limiter for API calls."""

    def __init__(self, requests_per_second: float) -> None:
        self._interval = 1.0 / requests_per_second
        self._last_call: float = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_call
        if elapsed < self._interval:
            time.sleep(self._interval - elapsed)
        self._last_call = time.monotonic()


# ---------------------------------------------------------------------------
# Retry with exponential backoff
# ---------------------------------------------------------------------------
def retry_with_backoff(
    func,
    max_retries: int = MAX_RETRIES,
    initial_backoff: float = INITIAL_BACKOFF_SECONDS,
    max_backoff: float = MAX_BACKOFF_SECONDS,
    backoff_factor: float = BACKOFF_FACTOR,
    retryable_status_codes: Tuple[int, ...] = (429, 500, 502, 503, 504),
):
    """Execute *func* with exponential backoff on retryable failures.

    Parameters
    ----------
    func : callable
        A zero-argument callable that performs the HTTP request and returns
        a ``requests.Response``.
    max_retries : int
        Maximum number of retry attempts.
    initial_backoff : float
        Base delay in seconds before the first retry.
    max_backoff : float
        Upper cap on the delay.
    backoff_factor : float
        Multiplier applied to the delay after each retry.
    retryable_status_codes : tuple of int
        HTTP status codes that should trigger a retry.

    Returns
    -------
    requests.Response
        The successful response.

    Raises
    ------
    requests.HTTPError
        When all retries are exhausted.
    """
    backoff = initial_backoff
    last_exception: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            response = func()

            if response.status_code == 200:
                return response

            if response.status_code in retryable_status_codes:
                # Respect Retry-After header when present
                retry_after = response.headers.get("Retry-After")
                wait_time = float(retry_after) if retry_after else backoff
                logger.warning(
                    "Retryable status %d on attempt %d/%d — waiting %.1fs",
                    response.status_code,
                    attempt,
                    max_retries,
                    wait_time,
                )
                time.sleep(wait_time)
                backoff = min(backoff * backoff_factor, max_backoff)
                continue

            # Non-retryable error
            response.raise_for_status()

        except requests.exceptions.ConnectionError as exc:
            last_exception = exc
            logger.warning(
                "Connection error on attempt %d/%d — waiting %.1fs: %s",
                attempt,
                max_retries,
                backoff,
                exc,
            )
            time.sleep(backoff)
            backoff = min(backoff * backoff_factor, max_backoff)

        except requests.exceptions.Timeout as exc:
            last_exception = exc
            logger.warning(
                "Timeout on attempt %d/%d — waiting %.1fs: %s",
                attempt,
                max_retries,
                backoff,
                exc,
            )
            time.sleep(backoff)
            backoff = min(backoff * backoff_factor, max_backoff)

    raise requests.HTTPError(
        f"All {max_retries} retries exhausted. Last error: {last_exception}"
    )


# ---------------------------------------------------------------------------
# Response validation
# ---------------------------------------------------------------------------
def validate_response(data: Dict[str, Any]) -> Tuple[List[Dict], List[Dict]]:
    """Validate API response records and separate valid from invalid.

    Returns
    -------
    (valid_records, dead_letter_records)
    """
    required_fields = {"event_id", "user_id", "event_type", "event_timestamp"}
    valid: List[Dict] = []
    dead_letter: List[Dict] = []

    records = data.get("data", data.get("results", []))
    if not isinstance(records, list):
        logger.error("Unexpected response structure — 'data'/'results' is not a list")
        return [], []

    for record in records:
        missing = required_fields - set(record.keys())
        if missing:
            record["_rejection_reason"] = f"missing fields: {missing}"
            record["_rejected_at"] = datetime.now(timezone.utc).isoformat()
            dead_letter.append(record)
        else:
            valid.append(record)

    if dead_letter:
        logger.warning(
            "Rejected %d of %d records (dead-letter queue)",
            len(dead_letter),
            len(records),
        )

    return valid, dead_letter


# ---------------------------------------------------------------------------
# Paginated API fetch
# ---------------------------------------------------------------------------
def fetch_all_pages(
    api_url: str,
    headers: Dict[str, str],
    page_size: int = DEFAULT_PAGE_SIZE,
    rate_limiter: Optional[RateLimiter] = None,
) -> Generator[Tuple[List[Dict], List[Dict]], None, None]:
    """Yield (valid_records, dead_letter_records) for every page of the API.

    Supports both cursor-based and offset-based pagination.
    """
    params: Dict[str, Any] = {"page_size": page_size}
    page = 0
    total_fetched = 0

    while True:
        page += 1
        if rate_limiter:
            rate_limiter.wait()

        logger.info("Fetching page %d (offset=%d) …", page, total_fetched)

        response = retry_with_backoff(
            lambda: requests.get(
                api_url, headers=headers, params=params, timeout=30
            )
        )
        body = response.json()

        valid, dead = validate_response(body)
        total_fetched += len(valid) + len(dead)
        yield valid, dead

        # Cursor-based pagination
        next_cursor = body.get("next_cursor") or body.get("pagination", {}).get(
            "next_cursor"
        )
        if next_cursor:
            params["cursor"] = next_cursor
            continue

        # Offset-based pagination
        total = body.get("total") or body.get("pagination", {}).get("total_count")
        if total is not None and total_fetched < total:
            params["offset"] = total_fetched
            continue

        # No more pages
        logger.info(
            "Pagination complete — %d records fetched across %d pages",
            total_fetched,
            page,
        )
        break


# ---------------------------------------------------------------------------
# DataFrame creation & schema enforcement
# ---------------------------------------------------------------------------
def create_dataframe(
    spark: SparkSession,
    records: List[Dict],
    schema: StructType,
) -> DataFrame:
    """Create a Spark DataFrame with enforced schema.

    Fields not in the schema are serialised into a catch-all JSON column.
    """
    schema_fields = {f.name for f in schema.fields}
    cleaned: List[Dict] = []

    for rec in records:
        row: Dict[str, Any] = {}
        extras: Dict[str, Any] = {}
        for k, v in rec.items():
            if k in schema_fields:
                row[k] = str(v) if v is not None else None
            else:
                extras[k] = v
        if extras:
            row.setdefault("event_properties", json.dumps(extras))
        cleaned.append(row)

    df = spark.createDataFrame(cleaned, schema=schema)
    return df


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------
def write_output(
    df: DataFrame,
    output_path: str,
    output_format: str,
    batch_date: str,
    mode: str = "append",
) -> int:
    """Write DataFrame to Delta Lake or Parquet with partitioning.

    Returns the number of rows written.
    """
    row_count = df.count()
    if row_count == 0:
        logger.warning("No records to write — skipping output")
        return 0

    df_with_meta = df.withColumn("_extracted_at", F.current_timestamp()).withColumn(
        "_batch_date", F.lit(batch_date)
    )

    writer = df_with_meta.write.mode(mode).partitionBy("_batch_date")

    if output_format == "delta":
        writer.format("delta").save(output_path)
    else:
        writer.format("parquet").option("compression", "snappy").save(output_path)

    logger.info(
        "Wrote %d records to %s (%s format)", row_count, output_path, output_format
    )
    return row_count


def write_dead_letter(
    spark: SparkSession,
    records: List[Dict],
    output_path: str,
    batch_date: str,
) -> None:
    """Persist rejected records for later inspection."""
    if not records:
        return

    dl_path = output_path.rstrip("/") + DEAD_LETTER_SUFFIX
    df = spark.createDataFrame(
        [{k: str(v) for k, v in r.items()} for r in records]
    )
    df = df.withColumn("_batch_date", F.lit(batch_date))
    df.write.mode("append").partitionBy("_batch_date").format("parquet").save(dl_path)
    logger.info("Wrote %d dead-letter records to %s", len(records), dl_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract data from a REST API and write to Delta Lake / Parquet."
    )
    parser.add_argument(
        "--api-url",
        required=True,
        help="Base URL of the API endpoint to extract from.",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        help="Target path (S3/HDFS/local) for extracted data.",
    )
    parser.add_argument(
        "--format",
        dest="output_format",
        choices=["delta", "parquet"],
        default="delta",
        help="Output storage format (default: delta).",
    )
    parser.add_argument(
        "--batch-date",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Logical batch date (default: today UTC).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"Number of records per API page (default: {DEFAULT_PAGE_SIZE}).",
    )
    parser.add_argument(
        "--api-key-env",
        default="API_KEY",
        help="Environment variable name holding the API key (default: API_KEY).",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)

    logger.info("Starting API extraction — url=%s date=%s", args.api_url, args.batch_date)

    # Resolve API key
    api_key = os.environ.get(args.api_key_env, "")
    headers = {
        "Accept": "application/json",
        "User-Agent": "data-engineering-pipelines/1.0",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    else:
        logger.warning("No API key found in env var '%s'", args.api_key_env)

    # Spark session
    spark = (
        SparkSession.builder.appName("extract_api_data")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    rate_limiter = RateLimiter(RATE_LIMIT_REQUESTS_PER_SECOND)
    all_valid: List[Dict] = []
    all_dead: List[Dict] = []

    try:
        for valid_batch, dead_batch in fetch_all_pages(
            api_url=args.api_url,
            headers=headers,
            page_size=args.page_size,
            rate_limiter=rate_limiter,
        ):
            all_valid.extend(valid_batch)
            all_dead.extend(dead_batch)

        if all_valid:
            df = create_dataframe(spark, all_valid, EVENTS_SCHEMA)
            rows_written = write_output(
                df, args.output_path, args.output_format, args.batch_date
            )
            logger.info("Extraction complete — %d rows written", rows_written)
        else:
            logger.warning("No valid records extracted")

        write_dead_letter(spark, all_dead, args.output_path, args.batch_date)

    except Exception:
        logger.exception("Fatal error during extraction")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
