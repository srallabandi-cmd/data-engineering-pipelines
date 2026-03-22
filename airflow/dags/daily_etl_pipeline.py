"""
Daily ETL Pipeline DAG

Orchestrates the complete daily data pipeline:
1. Sensor: Wait for source data arrival in S3
2. Extract: Pull data from REST APIs into staging
3. Transform: Run PySpark transformations
4. dbt: Build dimensional models
5. Quality: Run data quality checks
6. Notify: Send Slack notification on completion/failure

Schedule: Daily at 06:00 UTC
SLA: 4 hours (must complete by 10:00 UTC)
"""

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DAG_ID = "daily_etl_pipeline"
S3_BUCKET = Variable.get("s3_data_bucket", default_var="data-lake")
S3_STAGING_PREFIX = Variable.get("s3_staging_prefix", default_var="staging/events")
S3_PROCESSED_PREFIX = Variable.get("s3_processed_prefix", default_var="processed/events")
WAREHOUSE_PATH = Variable.get("warehouse_path", default_var="s3a://data-lake/warehouse/events")
DIMENSIONS_PATH = Variable.get("dimensions_path", default_var="s3a://data-lake/dimensions")
API_URL = Variable.get("events_api_url", default_var="https://api.example.com/v1/events")
API_KEY = Variable.get("events_api_key", default_var="")
SLACK_WEBHOOK_CONN_ID = "slack_webhook"
SPARK_CONN_ID = "spark_default"
DBT_PROJECT_DIR = Variable.get("dbt_project_dir", default_var="/opt/dbt")

# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------

def _on_failure_callback(context: Dict[str, Any]) -> None:
    """Send a Slack alert when a task fails."""
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")
    log_url = task_instance.log_url
    exception = context.get("exception")

    message = (
        f":red_circle: *Task Failed*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*Execution Date:* `{execution_date}`\n"
        f"*Error:* `{exception}`\n"
        f"<{log_url}|View Logs>"
    )

    slack = SlackWebhookOperator(
        task_id="failure_notification",
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        message=message,
    )
    slack.execute(context=context)


def _on_success_callback(context: Dict[str, Any]) -> None:
    """Log success metrics to XCom."""
    task_instance = context.get("task_instance")
    task_instance.xcom_push(
        key="completion_time",
        value=datetime.utcnow().isoformat(),
    )


# ---------------------------------------------------------------------------
# Task Functions
# ---------------------------------------------------------------------------

def extract_api_data(**context) -> Dict[str, Any]:
    """
    Extract data from the events API and push metadata to XCom.

    This function is executed by PythonOperator and calls the extraction
    logic from the spark-jobs module.
    """
    import json
    import sys
    sys.path.insert(0, "/opt/spark-jobs/etl")
    sys.path.insert(0, "/opt/spark-jobs/utils")

    from extract_api_data import APIExtractor, records_to_dataframe, write_to_staging, EVENTS_SCHEMA
    from spark_session import SparkSessionBuilder

    execution_date = context["ds"]
    output_path = f"s3a://{S3_BUCKET}/{S3_STAGING_PREFIX}"

    spark = SparkSessionBuilder(app_name="DailyETL_Extract").with_delta().with_s3().build()

    try:
        extractor = APIExtractor(
            base_url=API_URL,
            api_key=API_KEY,
            page_size=1000,
            requests_per_second=10.0,
        )
        records = extractor.extract(date_filter=execution_date)
        df = records_to_dataframe(spark, records, EVENTS_SCHEMA)
        records_written = write_to_staging(df, output_path, mode="append")

        extractor.metrics.records_written = records_written
        metrics = extractor.metrics.summary()

        context["task_instance"].xcom_push(key="extraction_metrics", value=json.dumps(metrics))
        context["task_instance"].xcom_push(key="records_extracted", value=records_written)
        context["task_instance"].xcom_push(key="staging_path", value=output_path)

        return metrics
    finally:
        spark.stop()


def run_quality_checks(**context) -> Dict[str, Any]:
    """
    Run data quality checks on the processed output.
    """
    import json
    import sys
    sys.path.insert(0, "/opt/spark-jobs/utils")

    from spark_session import SparkSessionBuilder
    from data_quality import DataQualityRunner, Severity

    execution_date = context["ds"]
    processed_path = f"s3a://{S3_BUCKET}/{S3_PROCESSED_PREFIX}"

    spark = SparkSessionBuilder(app_name="DailyETL_QualityChecks").with_delta().with_s3().build()

    try:
        df = spark.read.parquet(processed_path).filter(
            F.col("ingestion_date") == execution_date
        )

        runner = DataQualityRunner(spark, df, table_name="processed_events")
        runner.add_null_check("event_id", severity=Severity.CRITICAL)
        runner.add_null_check("event_type", severity=Severity.CRITICAL)
        runner.add_null_check("event_timestamp", severity=Severity.CRITICAL)
        runner.add_uniqueness_check("event_id")
        runner.add_range_check("revenue", min_val=0, max_val=1_000_000)
        runner.add_row_count_check(min_count=100)
        runner.add_accepted_values_check(
            "event_type",
            ["page_view", "click", "scroll", "purchase", "signup", "login", "logout"],
        )

        report = runner.run()

        context["task_instance"].xcom_push(
            key="quality_report", value=report.to_json(),
        )

        report.raise_on_failure()
        return report.summary()
    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "sla": timedelta(hours=4),
    "on_failure_callback": _on_failure_callback,
    "on_success_callback": _on_success_callback,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Daily ETL pipeline: extract, transform, load, quality check, and notify.",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "daily", "production"],
    doc_md=__doc__,
) as dag:

    # ---- 1. Data Arrival Sensor ----
    with TaskGroup(group_id="sensors") as sensor_group:
        wait_for_source_data = S3KeySensor(
            task_id="wait_for_source_data",
            bucket_name=S3_BUCKET,
            bucket_key=f"{S3_STAGING_PREFIX}/ingestion_date={{{{ ds }}}}/",
            wildcard_match=True,
            aws_conn_id="aws_default",
            poke_interval=300,
            timeout=3600,
            mode="reschedule",
            soft_fail=False,
        )

    # ---- 2. Extraction ----
    with TaskGroup(group_id="extraction") as extraction_group:
        extract_events = PythonOperator(
            task_id="extract_api_data",
            python_callable=extract_api_data,
            provide_context=True,
        )

    # ---- 3. Spark Transformations ----
    with TaskGroup(group_id="transformation") as transformation_group:
        transform_events = SparkSubmitOperator(
            task_id="transform_events",
            conn_id=SPARK_CONN_ID,
            application="/opt/spark-jobs/etl/transform_events.py",
            application_args=[
                "--input-path", f"s3a://{S3_BUCKET}/{S3_STAGING_PREFIX}",
                "--output-path", f"s3a://{S3_BUCKET}/{S3_PROCESSED_PREFIX}",
                "--dimensions-path", DIMENSIONS_PATH,
                "--date", "{{ ds }}",
                "--format", "parquet",
            ],
            conf={
                "spark.executor.memory": "8g",
                "spark.executor.cores": "4",
                "spark.sql.shuffle.partitions": "200",
                "spark.sql.adaptive.enabled": "true",
            },
            name="transform_events_{{ ds_nodash }}",
            verbose=True,
        )

        load_warehouse = SparkSubmitOperator(
            task_id="load_warehouse",
            conn_id=SPARK_CONN_ID,
            application="/opt/spark-jobs/etl/load_warehouse.py",
            application_args=[
                "--input-path", f"s3a://{S3_BUCKET}/{S3_PROCESSED_PREFIX}",
                "--warehouse-path", WAREHOUSE_PATH,
                "--load-type", "upsert",
                "--date", "{{ ds }}",
                "--merge-keys", "event_id",
            ],
            conf={
                "spark.executor.memory": "8g",
                "spark.executor.cores": "4",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            },
            name="load_warehouse_{{ ds_nodash }}",
            verbose=True,
        )

        transform_events >> load_warehouse

    # ---- 4. dbt Build ----
    with TaskGroup(group_id="dbt_build") as dbt_group:
        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=(
                f"cd {DBT_PROJECT_DIR} && "
                "dbt run "
                "--profiles-dir . "
                "--target prod "
                "--vars '{\"execution_date\": \"{{ ds }}\"}' "
                "--full-refresh={{ params.full_refresh }} "
                "2>&1"
            ),
            params={"full_refresh": "false"},
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=(
                f"cd {DBT_PROJECT_DIR} && "
                "dbt test "
                "--profiles-dir . "
                "--target prod "
                "2>&1"
            ),
        )

        dbt_run >> dbt_test

    # ---- 5. Quality Checks ----
    with TaskGroup(group_id="quality") as quality_group:
        quality_checks = PythonOperator(
            task_id="run_quality_checks",
            python_callable=run_quality_checks,
            provide_context=True,
        )

    # ---- 6. Notifications ----
    with TaskGroup(group_id="notifications") as notification_group:
        notify_success = SlackWebhookOperator(
            task_id="notify_success",
            slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
            message=(
                ":white_check_mark: *Daily ETL Pipeline Succeeded*\n"
                "*Date:* `{{ ds }}`\n"
                "*Records Extracted:* `{{ task_instance.xcom_pull(task_ids='extraction.extract_api_data', key='records_extracted') }}`\n"
                "*Duration:* `{{ dag_run.end_date }}`\n"
            ),
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        notify_failure = SlackWebhookOperator(
            task_id="notify_failure",
            slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
            message=(
                ":red_circle: *Daily ETL Pipeline Failed*\n"
                "*Date:* `{{ ds }}`\n"
                "*Check logs for details.*"
            ),
            trigger_rule=TriggerRule.ONE_FAILED,
        )

    # ---- Task Dependencies ----
    sensor_group >> extraction_group >> transformation_group >> dbt_group >> quality_group
    quality_group >> [notify_success, notify_failure]
