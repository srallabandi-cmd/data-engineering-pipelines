"""
Daily ETL Pipeline DAG
======================

Production Airflow DAG that orchestrates the daily batch ETL pipeline:

1. Wait for upstream dependencies (ExternalTaskSensor)
2. Extract data from the REST API
3. Transform events using PySpark (SparkSubmitOperator)
4. Load transformed data into the warehouse
5. Run data quality checks
6. Send Slack notifications on success/failure

Schedule: Daily at 06:00 UTC
SLA: Pipeline must complete within 3 hours of the scheduled start time.
"""

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

# ---------------------------------------------------------------------------
# Configuration from Airflow Variables (with sensible defaults)
# ---------------------------------------------------------------------------
SPARK_CONN_ID = Variable.get("spark_conn_id", default_var="spark_default")
SLACK_CONN_ID = Variable.get("slack_conn_id", default_var="slack_default")
DATA_LAKE_BASE = Variable.get("data_lake_base", default_var="s3a://data-lake")
WAREHOUSE_BASE = Variable.get("warehouse_base", default_var="s3a://data-warehouse")
API_URL = Variable.get("events_api_url", default_var="https://api.example.com/v1/events")

# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------

def _on_failure_callback(context: Dict[str, Any]) -> None:
    """Send a Slack alert when a task fails."""
    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = task_instance.task_id
    execution_date = context["execution_date"].isoformat()
    log_url = task_instance.log_url

    message = (
        f":red_circle: *Task Failed*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*Execution Date:* {execution_date}\n"
        f"*Log:* {log_url}"
    )
    SlackWebhookOperator(
        task_id="slack_failure_alert",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=message,
    ).execute(context=context)


def _on_success_callback(context: Dict[str, Any]) -> None:
    """Push a completion timestamp via XCom for downstream consumers."""
    task_instance = context["task_instance"]
    task_instance.xcom_push(
        key="completion_ts", value=datetime.utcnow().isoformat()
    )


def _sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """Alert on SLA breaches."""
    task_names = ", ".join(t.task_id for t in task_list)
    message = (
        f":warning: *SLA Miss*\n"
        f"*DAG:* `{dag.dag_id}`\n"
        f"*Tasks:* {task_names}"
    )
    SlackWebhookOperator(
        task_id="slack_sla_alert",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=message,
    ).execute(context={})


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def extract_api_data(**context) -> str:
    """Extract data from the events API and return the output path."""
    execution_date = context["ds"]
    output_path = f"{DATA_LAKE_BASE}/raw/events/_batch_date={execution_date}"

    import subprocess
    result = subprocess.run(
        [
            "spark-submit",
            "--packages", "io.delta:delta-core_2.12:2.4.0",
            "/opt/spark-jobs/etl/extract_api_data.py",
            "--api-url", API_URL,
            "--output-path", output_path,
            "--format", "delta",
            "--batch-date", execution_date,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    context["task_instance"].xcom_push(key="raw_path", value=output_path)
    context["task_instance"].xcom_push(key="batch_date", value=execution_date)
    return output_path


def load_to_warehouse(**context) -> str:
    """Load transformed data into the warehouse with MERGE semantics."""
    execution_date = context["ds"]
    input_path = f"{DATA_LAKE_BASE}/transformed/events"
    target_path = f"{WAREHOUSE_BASE}/events"

    import subprocess
    result = subprocess.run(
        [
            "spark-submit",
            "--packages", "io.delta:delta-core_2.12:2.4.0",
            "/opt/spark-jobs/etl/load_warehouse.py",
            "--input-path", input_path,
            "--target-path", target_path,
            "--batch-date", execution_date,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    context["task_instance"].xcom_push(key="warehouse_path", value=target_path)
    return target_path


def run_quality_checks(**context) -> Dict[str, Any]:
    """Run data quality checks on the warehouse table."""
    import json

    target_path = context["task_instance"].xcom_pull(
        task_ids="load_group.load_to_warehouse", key="warehouse_path"
    )

    # Run Great Expectations checkpoint
    import subprocess
    result = subprocess.run(
        [
            "python", "-m", "great_expectations",
            "checkpoint", "run", "daily_checkpoint",
        ],
        capture_output=True,
        text=True,
        cwd="/opt/data-quality/great_expectations",
    )

    report = {
        "target_path": target_path,
        "status": "passed" if result.returncode == 0 else "failed",
        "output": result.stdout[-500:] if result.stdout else "",
    }

    context["task_instance"].xcom_push(key="quality_report", value=json.dumps(report))

    if result.returncode != 0:
        raise RuntimeError(f"Data quality checks failed:\n{result.stderr}")

    return report


def send_success_notification(**context) -> None:
    """Send a Slack message summarising the successful pipeline run."""
    execution_date = context["ds"]
    quality_report = context["task_instance"].xcom_pull(
        task_ids="quality_group.run_quality_checks", key="quality_report"
    )

    message = (
        f":white_check_mark: *Daily ETL Pipeline Succeeded*\n"
        f"*Date:* {execution_date}\n"
        f"*Quality:* {quality_report or 'N/A'}"
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
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email": ["data-engineering@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "sla": timedelta(hours=3),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": _on_failure_callback,
    "on_success_callback": _on_success_callback,
}

with DAG(
    dag_id="daily_etl_pipeline",
    default_args=default_args,
    description="Daily batch ETL: API extract -> Spark transform -> Delta Lake load -> quality checks",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "production", "daily"],
    sla_miss_callback=_sla_miss_callback,
    doc_md=__doc__,
) as dag:

    # -- Sensor: wait for upstream data availability -----------------------
    wait_for_upstream = ExternalTaskSensor(
        task_id="wait_for_upstream",
        external_dag_id="upstream_data_ingestion",
        external_task_id="completion_marker",
        execution_delta=timedelta(hours=1),
        timeout=3600,
        poke_interval=120,
        mode="reschedule",
    )

    # -- Extract -----------------------------------------------------------
    with TaskGroup("extract_group", tooltip="Data extraction tasks") as extract_group:
        extract_task = PythonOperator(
            task_id="extract_api_data",
            python_callable=extract_api_data,
            provide_context=True,
        )

    # -- Transform ---------------------------------------------------------
    with TaskGroup("transform_group", tooltip="Spark transformation tasks") as transform_group:
        transform_task = SparkSubmitOperator(
            task_id="transform_events",
            application="/opt/spark-jobs/etl/transform_events.py",
            conn_id=SPARK_CONN_ID,
            packages="io.delta:delta-core_2.12:2.4.0",
            application_args=[
                "--input-path", f"{DATA_LAKE_BASE}/raw/events",
                "--output-path", f"{DATA_LAKE_BASE}/transformed/events",
                "--batch-date", "{{ ds }}",
            ],
            conf={
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.shuffle.partitions": "200",
            },
            executor_memory="8g",
            driver_memory="4g",
            num_executors=4,
            executor_cores=4,
            name="transform_events_{{ ds_nodash }}",
            verbose=False,
        )

    # -- Load --------------------------------------------------------------
    with TaskGroup("load_group", tooltip="Warehouse loading tasks") as load_group:
        load_task = PythonOperator(
            task_id="load_to_warehouse",
            python_callable=load_to_warehouse,
            provide_context=True,
        )

    # -- Quality -----------------------------------------------------------
    with TaskGroup("quality_group", tooltip="Data quality validation") as quality_group:
        quality_task = PythonOperator(
            task_id="run_quality_checks",
            python_callable=run_quality_checks,
            provide_context=True,
        )

    # -- Notification ------------------------------------------------------
    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=send_success_notification,
        provide_context=True,
        trigger_rule="all_success",
    )

    # -- Dependencies ------------------------------------------------------
    wait_for_upstream >> extract_group >> transform_group >> load_group >> quality_group >> notify_success
