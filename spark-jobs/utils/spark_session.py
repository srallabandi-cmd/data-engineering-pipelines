"""
Spark session builder with environment-specific configurations.

Provides a builder pattern for constructing SparkSession instances configured
for local development, dev, and production environments with support for
Delta Lake, S3/ADLS/GCS, Hive metastore, and performance tuning.

Usage:
    from spark_session import SparkSessionBuilder

    spark = (
        SparkSessionBuilder(app_name="MyJob")
        .with_delta()
        .with_s3("s3a://my-bucket")
        .with_hive()
        .build()
    )
"""

import logging
import os
from enum import Enum
from typing import Dict, Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class Environment(str, Enum):
    LOCAL = "local"
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


def detect_environment() -> Environment:
    """Detect the runtime environment from environment variables."""
    env_val = os.getenv("PIPELINE_ENV", os.getenv("SPARK_ENV", "local")).lower()
    try:
        return Environment(env_val)
    except ValueError:
        logger.warning("Unknown environment '%s'. Falling back to LOCAL.", env_val)
        return Environment.LOCAL


# ---------------------------------------------------------------------------
# Default configurations per environment
# ---------------------------------------------------------------------------
_ENV_DEFAULTS: Dict[Environment, Dict[str, str]] = {
    Environment.LOCAL: {
        "spark.master": "local[*]",
        "spark.driver.memory": "2g",
        "spark.executor.memory": "2g",
        "spark.sql.shuffle.partitions": "8",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.ui.enabled": "false",
        "spark.driver.bindAddress": "127.0.0.1",
    },
    Environment.DEV: {
        "spark.master": os.getenv("SPARK_MASTER", "spark://spark-master:7077"),
        "spark.driver.memory": "4g",
        "spark.executor.memory": "4g",
        "spark.executor.cores": "2",
        "spark.executor.instances": "2",
        "spark.sql.shuffle.partitions": "50",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.broadcastTimeout": "600",
    },
    Environment.STAGING: {
        "spark.master": os.getenv("SPARK_MASTER", "yarn"),
        "spark.driver.memory": "8g",
        "spark.executor.memory": "8g",
        "spark.executor.cores": "4",
        "spark.executor.instances": "4",
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.minExecutors": "2",
        "spark.dynamicAllocation.maxExecutors": "10",
    },
    Environment.PROD: {
        "spark.master": os.getenv("SPARK_MASTER", "yarn"),
        "spark.driver.memory": "16g",
        "spark.executor.memory": "16g",
        "spark.executor.cores": "4",
        "spark.executor.instances": "10",
        "spark.sql.shuffle.partitions": "400",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize": "64MB",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.broadcastTimeout": "600",
        "spark.sql.autoBroadcastJoinThreshold": "50MB",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.minExecutors": "5",
        "spark.dynamicAllocation.maxExecutors": "50",
        "spark.speculation": "true",
        "spark.speculation.multiplier": "2",
        "spark.speculation.quantile": "0.9",
        "spark.network.timeout": "600s",
        "spark.rpc.askTimeout": "600s",
    },
}


class SparkSessionBuilder:
    """
    Fluent builder for SparkSession with environment-aware defaults.

    Examples
    --------
    >>> spark = SparkSessionBuilder("MyApp").with_delta().with_s3().build()
    """

    def __init__(
        self,
        app_name: str = "DataEngineeringPipeline",
        environment: Optional[Environment] = None,
    ):
        self.app_name = app_name
        self.environment = environment or detect_environment()
        self._custom_configs: Dict[str, str] = {}
        self._enable_delta = False
        self._enable_hive = False
        self._enable_s3 = False
        self._enable_adls = False
        self._enable_gcs = False

        logger.info(
            "SparkSessionBuilder initialized: app=%s, env=%s",
            app_name, self.environment.value,
        )

    # ----- Cloud Storage -------------------------------------------------

    def with_s3(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
    ) -> "SparkSessionBuilder":
        """Configure S3/MinIO access."""
        self._enable_s3 = True
        endpoint = endpoint or os.getenv("S3_ENDPOINT", "http://minio:9000")
        access_key = access_key or os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
        secret_key = secret_key or os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

        self._custom_configs.update({
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": endpoint,
            "spark.hadoop.fs.s3a.access.key": access_key,
            "spark.hadoop.fs.s3a.secret.key": secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.hadoop.fs.s3a.fast.upload.buffer": "bytebuffer",
            "spark.hadoop.fs.s3a.multipart.size": "104857600",  # 100 MB
        })
        return self

    def with_adls(
        self,
        storage_account: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        tenant_id: Optional[str] = None,
    ) -> "SparkSessionBuilder":
        """Configure Azure Data Lake Storage Gen2 access."""
        self._enable_adls = True
        account = storage_account or os.getenv("ADLS_STORAGE_ACCOUNT")
        client = client_id or os.getenv("AZURE_CLIENT_ID")
        secret = client_secret or os.getenv("AZURE_CLIENT_SECRET")
        tenant = tenant_id or os.getenv("AZURE_TENANT_ID")

        if account:
            self._custom_configs.update({
                f"spark.hadoop.fs.azure.account.auth.type.{account}.dfs.core.windows.net": "OAuth",
                f"spark.hadoop.fs.azure.account.oauth.provider.type.{account}.dfs.core.windows.net":
                    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                f"spark.hadoop.fs.azure.account.oauth2.client.id.{account}.dfs.core.windows.net": client or "",
                f"spark.hadoop.fs.azure.account.oauth2.client.secret.{account}.dfs.core.windows.net": secret or "",
                f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{account}.dfs.core.windows.net":
                    f"https://login.microsoftonline.com/{tenant or ''}/oauth2/token",
            })
        return self

    def with_gcs(
        self,
        project_id: Optional[str] = None,
        credentials_file: Optional[str] = None,
    ) -> "SparkSessionBuilder":
        """Configure Google Cloud Storage access."""
        self._enable_gcs = True
        project = project_id or os.getenv("GCP_PROJECT_ID")
        creds = credentials_file or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        self._custom_configs.update({
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        })
        if project:
            self._custom_configs["spark.hadoop.fs.gs.project.id"] = project
        if creds:
            self._custom_configs["spark.hadoop.google.cloud.auth.service.account.json.keyfile"] = creds
        return self

    # ----- Features ------------------------------------------------------

    def with_delta(self) -> "SparkSessionBuilder":
        """Enable Delta Lake support."""
        self._enable_delta = True
        self._custom_configs.update({
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.databricks.delta.schema.autoMerge.enabled": "true",
            "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
            "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": "true",
        })
        return self

    def with_hive(self, metastore_uri: Optional[str] = None) -> "SparkSessionBuilder":
        """Enable Hive metastore integration."""
        self._enable_hive = True
        uri = metastore_uri or os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
        self._custom_configs["spark.hadoop.hive.metastore.uris"] = uri
        return self

    # ----- Custom Config -------------------------------------------------

    def with_config(self, key: str, value: str) -> "SparkSessionBuilder":
        """Set a custom Spark configuration."""
        self._custom_configs[key] = value
        return self

    def with_configs(self, configs: Dict[str, str]) -> "SparkSessionBuilder":
        """Set multiple custom Spark configurations."""
        self._custom_configs.update(configs)
        return self

    # ----- Build ---------------------------------------------------------

    def build(self) -> SparkSession:
        """Build and return the configured SparkSession."""
        env_configs = _ENV_DEFAULTS.get(self.environment, _ENV_DEFAULTS[Environment.LOCAL])

        builder = SparkSession.builder.appName(self.app_name)

        # Apply environment defaults
        for key, value in env_configs.items():
            builder = builder.config(key, value)

        # Apply custom / feature configs (override defaults)
        for key, value in self._custom_configs.items():
            builder = builder.config(key, value)

        # Enable Hive support
        if self._enable_hive:
            builder = builder.enableHiveSupport()

        spark = builder.getOrCreate()

        # Set log level based on environment
        log_level_map = {
            Environment.LOCAL: "WARN",
            Environment.DEV: "INFO",
            Environment.STAGING: "WARN",
            Environment.PROD: "WARN",
        }
        spark.sparkContext.setLogLevel(log_level_map.get(self.environment, "WARN"))

        logger.info(
            "SparkSession created: app=%s, env=%s, master=%s",
            self.app_name,
            self.environment.value,
            spark.sparkContext.master,
        )

        return spark
