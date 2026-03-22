"""
Data Quality Checks DAG
========================

Standalone DAG for running data quality checks across all critical tables.
Uses Great Expectations and custom quality checks with branching logic:
- If all checks pass: generate a summary report and notify success.
- If any check fails: send an alert and optionally quarantine bad data.

Schedule: Daily at 08:00 UTC (runs after the main ETL pipeline completes)
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SLACK_CONN_ID = Variable.get("slack_conn_id", default_var="slack_default")
WAREHOUSE_BASE = Variable.get("warehouse_base", default_var="s3a://data-warehouse")
GE_ROOT = "/opt/data-quality/great_expectations"

TABLES_TO_CHECK = [
    {"name": "events", "path": f"{WAREHOUSE_BASE}/events", "suite": "events_suite"},
    {"name": "users", "path": f"{WAREHOUSE_BASE}/users", "suite": "users_suite"},
    {"name": "sessions", "path": f"{WAREHOUSE_BASE}/sessions", "suite": "sessions_suite"},
]


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def run_ge_checkpoint(table_config: Dict[str, str], **context) -> Dict[str, Any]:
    """Run a Great Expectations checkpoint for a single table.

    Returns a result dict pushed via XCom.
    """
    import subprocess

    table_name = table_config["name"]
    suite_name = table_config["suite"]

    logger.info("Running GE checkpoint for table=%s suite=%s", table_name, suite_name)

    result = subprocess.run(
        [
            "python", "-m", "great_expectations",
            "checkpoint", "run", f"{table_name}_checkpoint",
        ],
        capture_output=True,
        text=True,
        cwd=GE_ROOT,
    )

    check_result = {
        "table": table_name,
        "suite": suite_name,
        "passed": result.returncode == 0,
        "stdout": result.stdout[-1000:] if result.stdout else "",
        "stderr": result.stderr[-500:] if result.stderr else "",
    }

    context["task_instance"].xcom_push(
        key=f"quality_{table_name}", value=json.dumps(check_result)
    )
    return check_result


def run_custom_quality_checks(table_config: Dict[str, str], **context) -> Dict[str, Any]:
    """Run custom PySpark-based quality checks for a single table."""
    from pyspark.sql import SparkSession

    table_name = table_config["name"]
    table_path = table_config["path"]

    spark = SparkSession.builder.appName(f"dq_{table_name}").getOrCreate()

    try:
        df = spark.read.format("delta").load(table_path)
        total_rows = df.count()

        # Basic checks
        from pyspark.sql import functions as F

        checks = {}

        # Row count check — must have data
        checks["row_count"] = total_rows > 0

        # Null checks on key columns
        if table_name == "events":
            null_event_id = df.filter(F.col("event_id").isNull()).count()
            null_user_id = df.filter(F.col("user_id").isNull()).count()
            checks["no_null_event_id"] = null_event_id == 0
            checks["no_null_user_id"] = null_user_id == 0

            # Freshness check — most recent event should be within 24h
            max_ts = df.agg(F.max("event_ts")).collect()[0][0]
            if max_ts:
                from datetime import timezone as tz
                age_hours = (datetime.now(tz.utc) - max_ts.replace(tzinfo=tz.utc)).total_seconds() / 3600
                checks["data_freshness_24h"] = age_hours <= 24

            # Uniqueness
            distinct_events = df.select("event_id").distinct().count()
            checks["event_id_unique"] = distinct_events == total_rows

        all_passed = all(checks.values())

        result = {
            "table": table_name,
            "total_rows": total_rows,
            "checks": checks,
            "all_passed": all_passed,
        }

        context["task_instance"].xcom_push(
            key=f"custom_quality_{table_name}", value=json.dumps(result, default=str)
        )
        return result

    finally:
        spark.stop()


def evaluate_results(**context) -> str:
    """Evaluate all quality check results and branch accordingly.

    Returns the downstream task_id to follow.
    """
    ti = context["task_instance"]
    all_passed = True

    for table_config in TABLES_TO_CHECK:
        table_name = table_config["name"]

        ge_result_raw = ti.xcom_pull(
            task_ids=f"ge_checks.ge_check_{table_name}",
            key=f"quality_{table_name}",
        )
        custom_result_raw = ti.xcom_pull(
            task_ids=f"custom_checks.custom_check_{table_name}",
            key=f"custom_quality_{table_name}",
        )

        if ge_result_raw:
            ge_result = json.loads(ge_result_raw)
            if not ge_result.get("passed", False):
                all_passed = False
                logger.warning("GE check failed for %s", table_name)

        if custom_result_raw:
            custom_result = json.loads(custom_result_raw)
            if not custom_result.get("all_passed", False):
                all_passed = False
                logger.warning("Custom check failed for %s", table_name)

    if all_passed:
        logger.info("All quality checks passed")
        return "notify_success"
    else:
        logger.warning("Quality checks failed — routing to failure handler")
        return "handle_failure"


def generate_quality_report(**context) -> str:
    """Generate a consolidated quality report and store it."""
    ti = context["task_instance"]
    execution_date = context["ds"]
    report_sections: List[Dict] = []

    for table_config in TABLES_TO_CHECK:
        table_name = table_config["name"]
        custom_raw = ti.xcom_pull(
            task_ids=f"custom_checks.custom_check_{table_name}",
            key=f"custom_quality_{table_name}",
        )
        if custom_raw:
            report_sections.append(json.loads(custom_raw))

    report = {
        "execution_date": execution_date,
        "generated_at": datetime.utcnow().isoformat(),
        "tables": report_sections,
        "overall_status": "passed" if all(
            s.get("all_passed", False) for s in report_sections
        ) else "failed",
    }

    report_json = json.dumps(report, indent=2, default=str)
    logger.info("Quality Report:\n%s", report_json)
    ti.xcom_push(key="quality_report", value=report_json)
    return report_json


def handle_failure_notification(**context) -> None:
    """Send detailed failure notification via Slack."""
    execution_date = context["ds"]
    message = (
        f":red_circle: *Data Quality Checks Failed*\n"
        f"*Date:* {execution_date}\n"
        f"*Action Required:* Review quality reports in Airflow logs and quarantine affected data.\n"
        f"*Dashboard:* <http://airflow:8080/dags/data_quality_checks|View DAG>"
    )
    SlackWebhookOperator(
        task_id="slack_failure",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=message,
    ).execute(context=context)


def send_success_notification(**context) -> None:
    """Send success notification via Slack."""
    execution_date = context["ds"]
    message = (
        f":white_check_mark: *Data Quality Checks Passed*\n"
        f"*Date:* {execution_date}\n"
        f"All tables validated successfully."
    )
    SlackWebhookOperator(
        task_id="slack_success",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=message,
    ).execute(context=context)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": ["data-engineering@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="data_quality_checks",
    default_args=default_args,
    description="Standalone data quality validation across all warehouse tables",
    schedule_interval="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["quality", "production", "daily"],
    doc_md=__doc__,
) as dag:

    # -- Great Expectations checks -----------------------------------------
    with TaskGroup("ge_checks", tooltip="Great Expectations validations") as ge_checks:
        for table_cfg in TABLES_TO_CHECK:
            PythonOperator(
                task_id=f"ge_check_{table_cfg['name']}",
                python_callable=run_ge_checkpoint,
                op_kwargs={"table_config": table_cfg},
                provide_context=True,
            )

    # -- Custom PySpark checks ---------------------------------------------
    with TaskGroup("custom_checks", tooltip="Custom PySpark quality checks") as custom_checks:
        for table_cfg in TABLES_TO_CHECK:
            PythonOperator(
                task_id=f"custom_check_{table_cfg['name']}",
                python_callable=run_custom_quality_checks,
                op_kwargs={"table_config": table_cfg},
                provide_context=True,
            )

    # -- Report generation -------------------------------------------------
    generate_report = PythonOperator(
        task_id="generate_quality_report",
        python_callable=generate_quality_report,
        provide_context=True,
    )

    # -- Branch based on results -------------------------------------------
    branch = BranchPythonOperator(
        task_id="evaluate_results",
        python_callable=evaluate_results,
        provide_context=True,
    )

    # -- Success path ------------------------------------------------------
    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=send_success_notification,
        provide_context=True,
    )

    # -- Failure path ------------------------------------------------------
    handle_failure = PythonOperator(
        task_id="handle_failure",
        python_callable=handle_failure_notification,
        provide_context=True,
    )

    # -- Dependencies ------------------------------------------------------
    [ge_checks, custom_checks] >> generate_report >> branch
    branch >> notify_success
    branch >> handle_failure
