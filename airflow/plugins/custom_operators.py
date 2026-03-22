"""
Custom Airflow Operators

Provides reusable operators for the data engineering platform:
- DataQualityOperator: Runs PySpark-based quality checks with fail thresholds
- SparkDeltaOperator: Submits Spark jobs pre-configured for Delta Lake
- SlackAlertOperator: Sends formatted Slack messages with full DAG context
"""

import json
import logging
from typing import Any, Dict, List, Optional, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.context import Context

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DataQualityOperator
# ---------------------------------------------------------------------------
class DataQualityOperator(BaseOperator):
    """
    Run data quality checks using the PySpark data quality framework.

    Connects to the data source, runs all configured checks, and fails
    the task if the number of critical failures exceeds the threshold.

    Parameters
    ----------
    table_path : str
        Path to the Delta/Parquet table to validate.
    checks : list of dict
        Each dict defines a check: {type, column, params, severity}.
        Supported types: null_check, uniqueness_check, range_check,
        row_count_check, accepted_values_check.
    fail_threshold : int
        Maximum allowed critical failures before the task fails (default: 0).
    spark_conn_id : str
        Airflow connection ID for Spark.
    date_filter : str, optional
        Filter the table to a specific date (column: ingestion_date).
    """

    template_fields: Sequence[str] = ("table_path", "date_filter")
    ui_color = "#FFCC00"
    ui_fgcolor = "#000000"

    def __init__(
        self,
        table_path: str,
        checks: List[Dict[str, Any]],
        fail_threshold: int = 0,
        spark_conn_id: str = "spark_default",
        date_filter: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.table_path = table_path
        self.checks = checks
        self.fail_threshold = fail_threshold
        self.spark_conn_id = spark_conn_id
        self.date_filter = date_filter

    def execute(self, context: Context) -> Dict[str, Any]:
        import sys
        sys.path.insert(0, "/opt/spark-jobs/utils")

        from spark_session import SparkSessionBuilder
        from data_quality import DataQualityRunner, Severity
        from pyspark.sql import functions as F

        spark = SparkSessionBuilder(app_name=f"DQ_{self.task_id}").with_delta().with_s3().build()

        try:
            df = spark.read.format("delta").load(self.table_path)
            if self.date_filter:
                df = df.filter(F.col("ingestion_date") == self.date_filter)

            runner = DataQualityRunner(spark, df, table_name=self.table_path)

            severity_map = {
                "critical": Severity.CRITICAL,
                "warning": Severity.WARNING,
                "info": Severity.INFO,
            }

            for check in self.checks:
                check_type = check["type"]
                column = check.get("column")
                params = check.get("params", {})
                severity = severity_map.get(check.get("severity", "critical"), Severity.CRITICAL)

                if check_type == "null_check":
                    runner.add_null_check(column, threshold=params.get("threshold", 0.0), severity=severity)
                elif check_type == "uniqueness_check":
                    runner.add_uniqueness_check(column, severity=severity)
                elif check_type == "range_check":
                    runner.add_range_check(
                        column,
                        min_val=params.get("min_val"),
                        max_val=params.get("max_val"),
                        severity=severity,
                    )
                elif check_type == "row_count_check":
                    runner.add_row_count_check(
                        min_count=params.get("min_count", 1),
                        max_count=params.get("max_count"),
                        severity=severity,
                    )
                elif check_type == "accepted_values_check":
                    runner.add_accepted_values_check(
                        column,
                        accepted_values=params.get("values", []),
                        severity=severity,
                    )
                else:
                    logger.warning("Unknown check type: %s. Skipping.", check_type)

            report = runner.run()

            # Push report to XCom
            context["task_instance"].xcom_push(key="quality_report", value=report.to_json())
            context["task_instance"].xcom_push(key="quality_summary", value=json.dumps(report.summary()))

            logger.info("Quality report summary: %s", json.dumps(report.summary()))

            if report.critical_failures > self.fail_threshold:
                raise AirflowException(
                    f"Data quality check failed: {report.critical_failures} critical failures "
                    f"(threshold: {self.fail_threshold}). "
                    f"Report: {report.to_json()}"
                )

            return report.summary()

        finally:
            spark.stop()


# ---------------------------------------------------------------------------
# SparkDeltaOperator
# ---------------------------------------------------------------------------
class SparkDeltaOperator(SparkSubmitOperator):
    """
    Submit a Spark job pre-configured with Delta Lake dependencies.

    Extends SparkSubmitOperator to automatically include Delta Lake
    packages, configurations, and environment-specific settings.

    Parameters
    ----------
    application : str
        Path to the Spark application (.py file).
    environment : str
        Target environment (local, dev, prod). Controls resource allocation.
    extra_spark_conf : dict, optional
        Additional Spark configuration to merge.
    delta_version : str
        Delta Lake package version (default: 3.1.0).
    """

    template_fields: Sequence[str] = SparkSubmitOperator.template_fields + ("environment",)
    ui_color = "#FF6600"

    _ENV_RESOURCES = {
        "local": {
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
            "spark.executor.instances": "1",
            "spark.sql.shuffle.partitions": "8",
        },
        "dev": {
            "spark.driver.memory": "4g",
            "spark.executor.memory": "4g",
            "spark.executor.cores": "2",
            "spark.executor.instances": "2",
            "spark.sql.shuffle.partitions": "50",
        },
        "prod": {
            "spark.driver.memory": "8g",
            "spark.executor.memory": "16g",
            "spark.executor.cores": "4",
            "spark.executor.instances": "10",
            "spark.sql.shuffle.partitions": "400",
            "spark.sql.adaptive.enabled": "true",
            "spark.dynamicAllocation.enabled": "true",
        },
    }

    def __init__(
        self,
        application: str,
        environment: str = "prod",
        extra_spark_conf: Optional[Dict[str, str]] = None,
        delta_version: str = "3.1.0",
        **kwargs,
    ):
        self.environment = environment

        # Build Delta Lake configuration
        delta_conf = {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.databricks.delta.schema.autoMerge.enabled": "true",
            "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
        }

        # Merge env resources
        env_conf = self._ENV_RESOURCES.get(environment, self._ENV_RESOURCES["prod"])
        merged_conf = {**env_conf, **delta_conf}
        if extra_spark_conf:
            merged_conf.update(extra_spark_conf)

        # Merge with any user-provided conf
        user_conf = kwargs.pop("conf", {}) or {}
        merged_conf.update(user_conf)

        # Set packages for Delta Lake
        packages = kwargs.pop("packages", None)
        if packages is None:
            packages = f"io.delta:delta-spark_2.12:{delta_version}"

        super().__init__(
            application=application,
            conf=merged_conf,
            packages=packages,
            **kwargs,
        )

    def execute(self, context: Context):
        logger.info(
            "Submitting Spark Delta job: app=%s, env=%s",
            self.application, self.environment,
        )
        return super().execute(context)


# ---------------------------------------------------------------------------
# SlackAlertOperator
# ---------------------------------------------------------------------------
class SlackAlertOperator(BaseOperator):
    """
    Send a formatted Slack message with full DAG execution context.

    Automatically includes DAG ID, task ID, execution date, run duration,
    and any custom message. Supports success, failure, and warning styles.

    Parameters
    ----------
    slack_webhook_conn_id : str
        Airflow connection ID for Slack webhook.
    message : str
        Custom message body.
    alert_type : str
        One of 'success', 'failure', 'warning', 'info'.
    include_log_link : bool
        Whether to include a link to the task log (default: True).
    extra_fields : dict, optional
        Additional key-value pairs to include in the message.
    """

    template_fields: Sequence[str] = ("message", "alert_type")
    ui_color = "#4A154B"

    _EMOJI_MAP = {
        "success": ":white_check_mark:",
        "failure": ":red_circle:",
        "warning": ":warning:",
        "info": ":information_source:",
    }

    _TITLE_MAP = {
        "success": "Success",
        "failure": "Failure",
        "warning": "Warning",
        "info": "Information",
    }

    def __init__(
        self,
        slack_webhook_conn_id: str = "slack_webhook",
        message: str = "",
        alert_type: str = "info",
        include_log_link: bool = True,
        extra_fields: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.slack_webhook_conn_id = slack_webhook_conn_id
        self.message = message
        self.alert_type = alert_type
        self.include_log_link = include_log_link
        self.extra_fields = extra_fields or {}

    def execute(self, context: Context) -> None:
        ti = context.get("task_instance")
        dag_id = context.get("dag").dag_id
        task_id = ti.task_id
        execution_date = context.get("execution_date")
        dag_run = context.get("dag_run")

        emoji = self._EMOJI_MAP.get(self.alert_type, ":speech_balloon:")
        title = self._TITLE_MAP.get(self.alert_type, "Alert")

        # Build message blocks
        lines = [
            f"{emoji} *{title}*",
            f"*DAG:* `{dag_id}`",
            f"*Task:* `{task_id}`",
            f"*Execution Date:* `{execution_date}`",
        ]

        if dag_run and dag_run.start_date and dag_run.end_date:
            duration = dag_run.end_date - dag_run.start_date
            lines.append(f"*Duration:* `{duration}`")

        if self.message:
            lines.append(f"*Message:* {self.message}")

        for key, value in self.extra_fields.items():
            lines.append(f"*{key}:* `{value}`")

        if self.include_log_link and ti:
            lines.append(f"<{ti.log_url}|View Logs>")

        full_message = "\n".join(lines)

        slack = SlackWebhookOperator(
            task_id=f"{self.task_id}_slack_inner",
            slack_webhook_conn_id=self.slack_webhook_conn_id,
            message=full_message,
        )
        slack.execute(context=context)

        logger.info("Slack %s alert sent for %s.%s", self.alert_type, dag_id, task_id)
