"""
Extract data from REST APIs into the staging layer.

This module provides a production-grade API extraction pipeline using PySpark.
It supports pagination, rate limiting, exponential backoff retries,
schema enforcement, and writes validated data as Parquet to the staging area.

Usage:
    spark-submit --master spark://master:7077 spark-jobs/etl/extract_api_data.py \
        --api-url https://api.example.com/v1/events \
        --output-path s3a://data-lake/staging/events \
        --date 2024-01-15
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, TimestampType, BooleanType, IntegerType,
)
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("extract_api_data")


# ---------------------------------------------------------------------------
# Metrics Collector
# ---------------------------------------------------------------------------
@dataclass
class ExtractionMetrics:
    """Tracks extraction run metrics."""
    records_fetched: int = 0
    records_written: int = 0
    pages_fetched: int = 0
    api_errors: int = 0
    retries: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None

    @property
    def duration_seconds(self) -> float:
        end = self.end_time or time.time()
        return round(end - self.start_time, 2)

    def summary(self) -> Dict[str, Any]:
        return {
            "records_fetched": self.records_fetched,
            "records_written": self.records_written,
            "pages_fetched": self.pages_fetched,
            "api_errors": self.api_errors,
            "retries": self.retries,
            "duration_seconds": self.duration_seconds,
        }


# ---------------------------------------------------------------------------
# Schema Definition
# ---------------------------------------------------------------------------
EVENTS_SCHEMA = StructType([
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
    StructField("properties", StringType(), nullable=True),
])


# ---------------------------------------------------------------------------
# HTTP Session Factory
# ---------------------------------------------------------------------------
def _build_http_session(
    max_retries: int = 5,
    backoff_factor: float = 1.0,
    status_forcelist: Tuple[int, ...] = (429, 500, 502, 503, 504),
    timeout: int = 30,
) -> requests.Session:
    """Build a requests.Session with retry and backoff."""
    session = requests.Session()
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=list(status_forcelist),
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.timeout = timeout
    return session


# ---------------------------------------------------------------------------
# Rate Limiter
# ---------------------------------------------------------------------------
class RateLimiter:
    """Token-bucket rate limiter."""

    def __init__(self, requests_per_second: float = 10.0):
        self._interval = 1.0 / requests_per_second
        self._last_request_time = 0.0

    def wait(self) -> None:
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._interval:
            time.sleep(self._interval - elapsed)
        self._last_request_time = time.time()


# ---------------------------------------------------------------------------
# API Client
# ---------------------------------------------------------------------------
class APIExtractor:
    """
    Extracts paginated data from a REST API endpoint.

    Handles pagination, rate limiting, retries, and response validation.
    """

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        page_size: int = 1000,
        max_pages: int = 500,
        requests_per_second: float = 10.0,
        request_timeout: int = 30,
        max_retries: int = 5,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.page_size = page_size
        self.max_pages = max_pages
        self.request_timeout = request_timeout
        self.rate_limiter = RateLimiter(requests_per_second)
        self.session = _build_http_session(
            max_retries=max_retries,
            timeout=request_timeout,
        )
        if api_key:
            self.session.headers.update({"Authorization": f"Bearer {api_key}"})
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "DataEngineeringPipelines/1.0",
        })
        self.metrics = ExtractionMetrics()

    # ----- request helpers ------------------------------------------------

    def _fetch_page(
        self, params: Dict[str, Any], attempt: int = 0
    ) -> Optional[requests.Response]:
        """Fetch a single page with manual exponential backoff on top of urllib3."""
        max_manual_retries = 3
        self.rate_limiter.wait()
        try:
            response = self.session.get(
                self.base_url, params=params, timeout=self.request_timeout,
            )
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                logger.warning("Rate limited. Sleeping %s seconds.", retry_after)
                time.sleep(retry_after)
                self.metrics.retries += 1
                if attempt < max_manual_retries:
                    return self._fetch_page(params, attempt + 1)
                return None

            if response.status_code >= 400:
                logger.error(
                    "API error %s: %s", response.status_code, response.text[:500],
                )
                self.metrics.api_errors += 1
                if response.status_code >= 500 and attempt < max_manual_retries:
                    wait = (2 ** attempt) + 0.5
                    logger.info("Retrying in %.1f seconds (attempt %d).", wait, attempt + 1)
                    time.sleep(wait)
                    self.metrics.retries += 1
                    return self._fetch_page(params, attempt + 1)
                return None

            return response

        except requests.exceptions.RequestException as exc:
            logger.error("Request failed: %s", exc)
            self.metrics.api_errors += 1
            if attempt < max_manual_retries:
                wait = (2 ** attempt) + 0.5
                time.sleep(wait)
                self.metrics.retries += 1
                return self._fetch_page(params, attempt + 1)
            return None

    @staticmethod
    def _validate_response(data: Any) -> bool:
        """Validate the response structure."""
        if not isinstance(data, dict):
            logger.error("Response is not a JSON object.")
            return False
        if "data" not in data:
            logger.error("Response missing 'data' key.")
            return False
        if not isinstance(data["data"], list):
            logger.error("'data' is not a list.")
            return False
        return True

    # ----- pagination -----------------------------------------------------

    def extract(
        self,
        date_filter: Optional[str] = None,
        extra_params: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Paginate through the API and collect all records.

        Parameters
        ----------
        date_filter : str, optional
            ISO-8601 date to filter results (passed as query param ``date``).
        extra_params : dict, optional
            Additional query parameters.

        Returns
        -------
        list of dict
            Collected records across all pages.
        """
        all_records: List[Dict[str, Any]] = []
        page = 1
        has_more = True

        base_params: Dict[str, Any] = {"page_size": self.page_size}
        if date_filter:
            base_params["date"] = date_filter
        if extra_params:
            base_params.update(extra_params)

        logger.info(
            "Starting extraction from %s (page_size=%d, date=%s)",
            self.base_url, self.page_size, date_filter,
        )

        while has_more and page <= self.max_pages:
            params = {**base_params, "page": page}
            response = self._fetch_page(params)
            if response is None:
                logger.error("Failed to fetch page %d. Stopping pagination.", page)
                break

            try:
                payload = response.json()
            except (json.JSONDecodeError, ValueError):
                logger.error("Invalid JSON on page %d.", page)
                self.metrics.api_errors += 1
                break

            if not self._validate_response(payload):
                break

            records = payload["data"]
            record_count = len(records)
            all_records.extend(records)
            self.metrics.records_fetched += record_count
            self.metrics.pages_fetched += 1

            logger.info(
                "Page %d: fetched %d records (total: %d).",
                page, record_count, self.metrics.records_fetched,
            )

            # Determine if there are more pages
            pagination = payload.get("pagination", payload.get("meta", {}))
            has_more = pagination.get("has_more", record_count >= self.page_size)
            page += 1

        logger.info(
            "Extraction complete. Total records: %d across %d pages.",
            self.metrics.records_fetched, self.metrics.pages_fetched,
        )
        return all_records


# ---------------------------------------------------------------------------
# Spark ingestion
# ---------------------------------------------------------------------------
def records_to_dataframe(
    spark: SparkSession,
    records: List[Dict[str, Any]],
    schema: StructType,
) -> DataFrame:
    """
    Convert raw API records into a validated Spark DataFrame.

    Applies schema enforcement, drops malformed rows, and adds metadata columns.
    """
    if not records:
        logger.warning("No records to convert. Returning empty DataFrame.")
        return spark.createDataFrame([], schema)

    raw_df = spark.createDataFrame(records, schema=schema)

    validated_df = (
        raw_df
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("event_type").isNotNull())
        .filter(F.col("timestamp").isNotNull())
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
        .drop("timestamp")
    )

    return validated_df


def write_to_staging(
    df: DataFrame,
    output_path: str,
    partition_cols: Optional[List[str]] = None,
    mode: str = "append",
    file_format: str = "parquet",
) -> int:
    """
    Write a DataFrame to the staging area.

    Parameters
    ----------
    df : DataFrame
    output_path : str
        Target path (e.g., s3a://bucket/staging/events).
    partition_cols : list of str, optional
        Columns to partition by (default: ingestion_date, event_type).
    mode : str
        Save mode (append, overwrite, etc.).
    file_format : str
        Output format (parquet or delta).

    Returns
    -------
    int
        Number of records written.
    """
    if partition_cols is None:
        partition_cols = ["ingestion_date", "event_type"]

    record_count = df.count()
    if record_count == 0:
        logger.warning("DataFrame is empty. Nothing to write.")
        return 0

    logger.info(
        "Writing %d records to %s (format=%s, mode=%s, partitions=%s).",
        record_count, output_path, file_format, mode, partition_cols,
    )

    writer = df.repartition(*[F.col(c) for c in partition_cols]).write.mode(mode)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    if file_format == "delta":
        writer.format("delta").save(output_path)
    else:
        writer.option("compression", "snappy").parquet(output_path)

    logger.info("Write complete: %d records to %s.", record_count, output_path)
    return record_count


# ---------------------------------------------------------------------------
# CLI Entrypoint
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract API data to staging.")
    parser.add_argument("--api-url", required=True, help="API endpoint URL.")
    parser.add_argument("--api-key", default=None, help="Bearer token for API auth.")
    parser.add_argument("--output-path", required=True, help="Staging output path.")
    parser.add_argument("--date", default=None, help="Date filter (YYYY-MM-DD).")
    parser.add_argument("--page-size", type=int, default=1000, help="Records per page.")
    parser.add_argument("--max-pages", type=int, default=500, help="Max pages to fetch.")
    parser.add_argument("--rps", type=float, default=10.0, help="Requests per second limit.")
    parser.add_argument("--format", choices=["parquet", "delta"], default="parquet")
    parser.add_argument("--mode", default="append", help="Write mode.")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)

    # Import here to allow the module to be importable without Spark context
    from spark_session import SparkSessionBuilder  # noqa: local util

    spark = (
        SparkSessionBuilder(app_name="ExtractAPIData")
        .with_delta()
        .build()
    )

    try:
        extractor = APIExtractor(
            base_url=args.api_url,
            api_key=args.api_key,
            page_size=args.page_size,
            max_pages=args.max_pages,
            requests_per_second=args.rps,
        )

        records = extractor.extract(date_filter=args.date)

        df = records_to_dataframe(spark, records, EVENTS_SCHEMA)
        records_written = write_to_staging(
            df, args.output_path, mode=args.mode, file_format=args.format,
        )

        extractor.metrics.records_written = records_written
        extractor.metrics.end_time = time.time()

        logger.info("Extraction metrics: %s", json.dumps(extractor.metrics.summary()))

        if extractor.metrics.api_errors > 0:
            logger.warning(
                "Completed with %d API errors.", extractor.metrics.api_errors,
            )

    except Exception:
        logger.exception("Extraction job failed.")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
