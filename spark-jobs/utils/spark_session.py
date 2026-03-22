"""
SparkSession Builder
====================

Configurable SparkSession factory with Delta Lake extensions, Hive metastore
support, performance tuning, and environment-based configuration (dev / prod).

Usage:
    from spark_session import get_spark_session
    spark = get_spark_session(app_name="my_job", env="prod")
"""

import logging
import os
import sys
from enum import Enum
from typing import Dict, Optional

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("spark_session")


# ---------------------------------------------------------------------------
# Environment enum
# ---------------------------------------------------------------------------
class Environment(str, Enum):
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


# ---------------------------------------------------------------------------
# Per-environment defaults
# ---------------------------------------------------------------------------
BASE_CONFIG: Dict[str, str] = {
    # Delta Lake
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    # Adaptive Query Execution
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    # Schema evolution
    "spark.databricks.delta.schema.autoMerge.enabled": "true",
    # Misc
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.parquet.compression.codec": "snappy",
}

ENV_OVERRIDES: Dict[str, Dict[str, str]] = {
    Environment.DEV: {
        "spark.master": "local[*]",
        "spark.driver.memory": "2g",
        "spark.executor.memory": "2g",
        "spark.sql.shuffle.partitions": "8",
        "spark.sql.autoBroadcastJoinThreshold": str(10 * 1024 * 1024),  # 10 MB
        "spark.ui.enabled": "true",
    },
    Environment.STAGING: {
        "spark.driver.memory": "4g",
        "spark.executor.memory": "8g",
        "spark.executor.cores": "4",
        "spark.sql.shuffle.partitions": "100",
        "spark.sql.autoBroadcastJoinThreshold": str(50 * 1024 * 1024),  # 50 MB
    },
    Environment.PROD: {
        "spark.driver.memory": "8g",
        "spark.executor.memory": "16g",
        "spark.executor.cores": "4",
        "spark.executor.instances": "10",
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.autoBroadcastJoinThreshold": str(100 * 1024 * 1024),  # 100 MB
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.minExecutors": "2",
        "spark.dynamicAllocation.maxExecutors": "20",
        "spark.speculation": "true",
    },
}

# S3 / MinIO defaults (overridden by environment variables)
S3_CONFIG: Dict[str, str] = {
    "spark.hadoop.fs.s3a.endpoint": os.getenv("S3_ENDPOINT", "http://minio:9000"),
    "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
}

# Hive metastore (only enabled when HIVE_METASTORE_URI is set)
HIVE_METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "")


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------
def get_spark_session(
    app_name: str = "data_engineering_pipeline",
    env: Optional[str] = None,
    extra_config: Optional[Dict[str, str]] = None,
    enable_hive: bool = False,
    log_level: str = "WARN",
) -> SparkSession:
    """Create or retrieve a SparkSession with opinionated production defaults.

    Parameters
    ----------
    app_name : str
        Application name shown in the Spark UI.
    env : str or None
        One of ``dev``, ``staging``, ``prod``.  Falls back to the
        ``SPARK_ENV`` environment variable, then ``dev``.
    extra_config : dict, optional
        Additional Spark config key-value pairs that override defaults.
    enable_hive : bool
        Whether to enable Hive metastore support.
    log_level : str
        Spark log level (e.g. ``WARN``, ``INFO``, ``ERROR``).

    Returns
    -------
    SparkSession
    """
    env = Environment(env or os.getenv("SPARK_ENV", "dev"))
    logger.info("Building SparkSession: app=%s env=%s hive=%s", app_name, env, enable_hive)

    # Merge configs: base -> env -> s3 -> extra
    merged: Dict[str, str] = {}
    merged.update(BASE_CONFIG)
    merged.update(ENV_OVERRIDES.get(env, {}))
    merged.update(S3_CONFIG)
    if extra_config:
        merged.update(extra_config)

    builder = SparkSession.builder.appName(app_name)

    for key, value in merged.items():
        builder = builder.config(key, value)

    # Hive support
    if enable_hive or HIVE_METASTORE_URI:
        if HIVE_METASTORE_URI:
            builder = builder.config(
                "spark.hadoop.hive.metastore.uris", HIVE_METASTORE_URI
            )
        builder = builder.enableHiveSupport()
        logger.info("Hive support enabled (metastore=%s)", HIVE_METASTORE_URI or "embedded")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)

    # Log effective config for debugging
    effective = {k: spark.conf.get(k) for k in sorted(merged.keys())}
    logger.info("Effective Spark config: %s", effective)

    return spark


def stop_spark(spark: SparkSession) -> None:
    """Gracefully stop the SparkSession."""
    logger.info("Stopping SparkSession")
    spark.stop()
    logger.info("SparkSession stopped")
