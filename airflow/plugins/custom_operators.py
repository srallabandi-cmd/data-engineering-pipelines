"""
Custom Airflow Operators
========================

Production-ready custom operators for the data engineering platform:

- DataQualityOperator: Runs data quality checks and raises on failure.
- SparkDeltaOperator: Submits Spark jobs with Delta Lake configuration.
- SlackAlertOperator: Sends formatted Slack notifications.
"""

import json
import logging
import subprocess
from datetime import datetime
from typing import Any, Dict, List, Optional

from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DataQualityOperator
# ---------------------------------------------------------------------------
class DataQualityOperator(BaseOperator):
    """Run data quality checks against a Delta Lake table and raise on failure.

    Parameters
    ----------
    table_path : str
        Path to the Delta Lake table to validate.
    checks : list of dict
        Each dict has keys: ``column``, ``check_type`` (null, unique, range,
        row_count), and check-specific parameters.
    fail_on_error : bool
        Whether to raise an ``AirflowFailException`` on check failure.
    """

    template_fields = ("table_path",)
    ui_color = "#e8f7e4"

    @apply_defaults
    def __init__(
        self,
        table_path: str,
        checks: List[Dict[str, Any]],
        fail_on_error: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_path = table_path
        self.checks = checks
        self.fail_on_error = fail_on_error

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = SparkSession.builder.appName(
            f"dq_{self.task_id}"
        ).getOrCreate()

        try:
            df = spark.read.format("delta").load(self.table_path)
            total_rows = df.count()
            results: List[Dict[str, Any]] = []

            for check_def in self.checks:
                column = check_def.get("column")
                check_type = check_def["check_type"]

                if check_type == "null":
                    max_pct = check_def.get("max_null_pct", 0.0)
                    null_count = df.filter(F.col(column).isNull()).count()
                    null_pct = null_count / total_rows if total_rows else 1.0
                    passed = null_pct <= max_pct
                    results.append({
                        "check": f"null_{column}",
                        "passed": passed,
                        "metric": round(null_pct, 6),
                        "threshold": max_pct,
                    })

                elif check_type == "unique":
                    distinct = df.select(column).distinct().count()
                    dups = total_rows - distinct
                    passed = dups == 0
                    results.append({
                        "check": f"unique_{column}",
                        "passed": passed,
                        "metric": dups,
                        "threshold": 0,
                    })

                elif check_type == "range":
                    min_val = check_def.get("min_val")
                    max_val = check_def.get("max_val")
                    cond = F.lit(False)
                    if min_val is not None:
                        cond = cond | (F.col(column) < min_val)
                    if max_val is not None:
                        cond = cond | (F.col(column) > max_val)
                    violations = df.filter(F.col(column).isNotNull()).filter(cond).count()
                    passed = violations == 0
                    results.append({
                        "check": f"range_{column}",
                        "passed": passed,
                        "metric": violations,
                        "threshold": f"[{min_val}, {max_val}]",
                    })

                elif check_type == "row_count":
                    min_rows = check_def.get("min_rows", 1)
                    passed = total_rows >= min_rows
                    results.append({
                        "check": "row_count",
                        "passed": passed,
                        "metric": total_rows,
                        "threshold": min_rows,
                    })

            all_passed = all(r["passed"] for r in results)
            report = {
                "table_path": self.table_path,
                "total_rows": total_rows,
                "results": results,
                "all_passed": all_passed,
            }

            logger.info("Quality report: %s", json.dumps(report, indent=2))
            context["task_instance"].xcom_push(
                key="quality_report", value=json.dumps(report)
            )

            if not all_passed and self.fail_on_error:
                failures = [r for r in results if not r["passed"]]
                raise RuntimeError(
                    f"Data quality checks failed: {json.dumps(failures)}"
                )

            return report

        finally:
            spark.stop()


# ---------------------------------------------------------------------------
# SparkDeltaOperator
# ---------------------------------------------------------------------------
class SparkDeltaOperator(BaseOperator):
    """Submit a Spark job with Delta Lake configuration pre-applied.

    Parameters
    ----------
    application : str
        Path to the PySpark script.
    application_args : list of str
        Arguments passed to the Spark application.
    spark_conf : dict, optional
        Additional Spark configuration key-value pairs.
    executor_memory : str
        Executor memory (default: 4g).
    driver_memory : str
        Driver memory (default: 2g).
    num_executors : int
        Number of executors (default: 2).
    packages : str
        Comma-separated Maven coordinates of packages.
    """

    template_fields = ("application", "application_args")
    ui_color = "#d4e6f1"

    DELTA_PACKAGES = "io.delta:delta-core_2.12:2.4.0"
    DELTA_CONF = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.adaptive.enabled": "true",
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
    }

    @apply_defaults
    def __init__(
        self,
        application: str,
        application_args: Optional[List[str]] = None,
        spark_conf: Optional[Dict[str, str]] = None,
        executor_memory: str = "4g",
        driver_memory: str = "2g",
        num_executors: int = 2,
        packages: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application = application
        self.application_args = application_args or []
        self.spark_conf = spark_conf or {}
        self.executor_memory = executor_memory
        self.driver_memory = driver_memory
        self.num_executors = num_executors
        self.packages = packages or self.DELTA_PACKAGES

    def execute(self, context: Dict[str, Any]) -> str:
        merged_conf = {**self.DELTA_CONF, **self.spark_conf}

        cmd = [
            "spark-submit",
            "--packages", self.packages,
            "--executor-memory", self.executor_memory,
            "--driver-memory", self.driver_memory,
            "--num-executors", str(self.num_executors),
        ]

        for key, value in merged_conf.items():
            cmd.extend(["--conf", f"{key}={value}"])

        cmd.append(self.application)
        cmd.extend(self.application_args)

        logger.info("Running: %s", " ".join(cmd))

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            logger.error("Spark job failed:\nSTDOUT: %s\nSTDERR: %s", result.stdout[-2000:], result.stderr[-2000:])
            raise RuntimeError(
                f"Spark job {self.application} failed with exit code {result.returncode}"
            )

        logger.info("Spark job completed successfully")
        return result.stdout[-500:]


# ---------------------------------------------------------------------------
# SlackAlertOperator
# ---------------------------------------------------------------------------
class SlackAlertOperator(BaseOperator):
    """Send a formatted Slack notification.

    Parameters
    ----------
    message_template : str
        Message template with ``{dag_id}``, ``{task_id}``, ``{execution_date}``,
        ``{status}``, and ``{details}`` placeholders.
    status : str
        One of ``success``, ``failure``, ``warning``, ``info``.
    details : str, optional
        Extra details to include in the message.
    slack_conn_id : str
        Airflow connection ID for the Slack webhook.
    """

    template_fields = ("message_template", "details")
    ui_color = "#fce4ec"

    STATUS_EMOJI = {
        "success": ":white_check_mark:",
        "failure": ":red_circle:",
        "warning": ":warning:",
        "info": ":information_source:",
    }

    @apply_defaults
    def __init__(
        self,
        message_template: Optional[str] = None,
        status: str = "info",
        details: str = "",
        slack_conn_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.message_template = message_template or (
            "{emoji} *Pipeline {status}*\n"
            "*DAG:* `{dag_id}`\n"
            "*Task:* `{task_id}`\n"
            "*Date:* {execution_date}\n"
            "{details}"
        )
        self.status = status
        self.details = details
        self.slack_conn_id = slack_conn_id or Variable.get(
            "slack_conn_id", default_var="slack_default"
        )

    def execute(self, context: Dict[str, Any]) -> None:
        from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

        emoji = self.STATUS_EMOJI.get(self.status, ":grey_question:")
        message = self.message_template.format(
            emoji=emoji,
            status=self.status.upper(),
            dag_id=context["dag"].dag_id,
            task_id=self.task_id,
            execution_date=context["ds"],
            details=self.details,
        )

        SlackWebhookOperator(
            task_id=f"slack_alert_{self.task_id}",
            slack_webhook_conn_id=self.slack_conn_id,
            message=message,
        ).execute(context=context)

        logger.info("Slack alert sent: status=%s", self.status)
