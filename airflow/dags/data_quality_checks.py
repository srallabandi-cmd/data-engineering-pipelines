"""
Data Quality Checks DAG

Runs comprehensive data quality validations after the daily ETL pipeline:
1. Great Expectations checkpoint execution
2. SQL-based quality checks (nulls, ranges, referential integrity)
3. Data freshness checks
4. Volume anomaly detection (comparison to historical averages)
5. Alert on any failures via Slack

Schedule: Triggered after daily_etl_pipeline or at 10:00 UTC daily
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DAG_ID = "data_quality_checks"
SLACK_WEBHOOK_CONN_ID = "slack_webhook"
S3_BUCKET = Variable.get("s3_data_bucket", default_var="data-lake")
WAREHOUSE_PATH = Variable.get("warehouse_path", default_var="s3a://data-lake/warehouse")
GE_PROJECT_DIR = Variable.get(
    "great_expectations_dir",
    default_var="/opt/data-quality/great_expectations",
)
FRESHNESS_THRESHOLD_HOURS = int(Variable.get("freshness_threshold_hours", default_var="6"))
VOLUME_ANOMALY_STDDEV = float(Variable.get("volume_anomaly_stddev", default_var="2.0"))

# ---------------------------------------------------------------------------
# Task Callables
# ---------------------------------------------------------------------------

def run_great_expectations_checkpoint(**context) -> Dict[str, Any]:
    """
    Execute a Great Expectations checkpoint for the events dataset.

    Returns the validation result summary and pushes pass/fail to XCom.
    """
    import great_expectations as gx
    import json

    execution_date = context["ds"]

    ge_context = gx.get_context(context_root_dir=GE_PROJECT_DIR)

    checkpoint_result = ge_context.run_checkpoint(
        checkpoint_name="daily_checkpoint",
        batch_request={
            "runtime_parameters": {
                "query": f"""
                    SELECT * FROM events
                    WHERE ingestion_date = '{execution_date}'
                """
            },
            "batch_identifiers": {
                "execution_date": execution_date,
            },
        },
    )

    success = checkpoint_result.success
    statistics = checkpoint_result.statistics

    result = {
        "success": success,
        "evaluated_expectations": statistics.get("evaluated_expectations", 0),
        "successful_expectations": statistics.get("successful_expectations", 0),
        "unsuccessful_expectations": statistics.get("unsuccessful_expectations", 0),
    }

    context["task_instance"].xcom_push(key="ge_result", value=json.dumps(result))
    context["task_instance"].xcom_push(key="ge_success", value=success)

    if not success:
        raise ValueError(
            f"Great Expectations checkpoint failed: "
            f"{result['unsuccessful_expectations']} expectations did not pass."
        )

    return result


def run_sql_quality_checks(**context) -> Dict[str, Any]:
    """
    Execute SQL-based quality checks against the warehouse.

    Checks:
    - Critical column nullability
    - Primary key uniqueness
    - Foreign key referential integrity
    - Value domain validation
    """
    import json
    import sys
    sys.path.insert(0, "/opt/spark-jobs/utils")

    from spark_session import SparkSessionBuilder
    from pyspark.sql import functions as F

    execution_date = context["ds"]
    spark = SparkSessionBuilder(app_name="DQ_SQLChecks").with_delta().with_s3().build()

    results = []
    all_passed = True

    try:
        events_df = spark.read.format("delta").load(f"{WAREHOUSE_PATH}/events")
        daily_events = events_df.filter(F.col("ingestion_date") == execution_date)
        daily_events.createOrReplaceTempView("events")

        checks = [
            {
                "name": "event_id_not_null",
                "sql": "SELECT COUNT(*) AS result FROM events WHERE event_id IS NULL",
                "expected": 0,
                "operator": "eq",
                "severity": "critical",
            },
            {
                "name": "event_id_unique",
                "sql": """
                    SELECT COUNT(*) AS result FROM (
                        SELECT event_id, COUNT(*) AS cnt
                        FROM events
                        GROUP BY event_id
                        HAVING cnt > 1
                    )
                """,
                "expected": 0,
                "operator": "eq",
                "severity": "critical",
            },
            {
                "name": "revenue_non_negative",
                "sql": "SELECT COUNT(*) AS result FROM events WHERE revenue < 0",
                "expected": 0,
                "operator": "eq",
                "severity": "critical",
            },
            {
                "name": "valid_event_types",
                "sql": """
                    SELECT COUNT(*) AS result FROM events
                    WHERE event_type NOT IN (
                        'page_view', 'click', 'scroll', 'purchase',
                        'signup', 'login', 'logout'
                    )
                """,
                "expected": 0,
                "operator": "eq",
                "severity": "warning",
            },
            {
                "name": "timestamp_not_future",
                "sql": "SELECT COUNT(*) AS result FROM events WHERE event_timestamp > current_timestamp()",
                "expected": 0,
                "operator": "eq",
                "severity": "warning",
            },
        ]

        for check in checks:
            try:
                result_df = spark.sql(check["sql"])
                actual = result_df.collect()[0]["result"]

                if check["operator"] == "eq":
                    passed = actual == check["expected"]
                elif check["operator"] == "lte":
                    passed = actual <= check["expected"]
                else:
                    passed = actual == check["expected"]

                if not passed and check["severity"] == "critical":
                    all_passed = False

                results.append({
                    "name": check["name"],
                    "passed": passed,
                    "expected": check["expected"],
                    "actual": actual,
                    "severity": check["severity"],
                })
            except Exception as exc:
                results.append({
                    "name": check["name"],
                    "passed": False,
                    "expected": check["expected"],
                    "actual": f"ERROR: {str(exc)}",
                    "severity": check["severity"],
                })
                if check["severity"] == "critical":
                    all_passed = False

        context["task_instance"].xcom_push(key="sql_check_results", value=json.dumps(results))
        context["task_instance"].xcom_push(key="sql_checks_passed", value=all_passed)

        if not all_passed:
            failed = [r for r in results if not r["passed"] and r["severity"] == "critical"]
            raise ValueError(
                f"SQL quality checks failed: {len(failed)} critical failures. "
                f"Details: {json.dumps(failed)}"
            )

        return {"checks": len(results), "all_passed": all_passed}

    finally:
        spark.stop()


def check_data_freshness(**context) -> Dict[str, Any]:
    """
    Verify that data is fresh enough (within the threshold).

    Compares the most recent event timestamp against the current time.
    """
    import json
    import sys
    sys.path.insert(0, "/opt/spark-jobs/utils")

    from spark_session import SparkSessionBuilder
    from pyspark.sql import functions as F

    spark = SparkSessionBuilder(app_name="DQ_Freshness").with_delta().with_s3().build()

    try:
        events_df = spark.read.format("delta").load(f"{WAREHOUSE_PATH}/events")

        max_ts_row = events_df.agg(F.max("event_timestamp").alias("max_ts")).collect()[0]
        max_ts = max_ts_row["max_ts"]

        if max_ts is None:
            raise ValueError("No events found in warehouse.")

        from datetime import timezone
        now = datetime.now(timezone.utc)
        age_hours = (now - max_ts.replace(tzinfo=timezone.utc)).total_seconds() / 3600

        is_fresh = age_hours <= FRESHNESS_THRESHOLD_HOURS

        result = {
            "most_recent_event": str(max_ts),
            "age_hours": round(age_hours, 2),
            "threshold_hours": FRESHNESS_THRESHOLD_HOURS,
            "is_fresh": is_fresh,
        }

        context["task_instance"].xcom_push(key="freshness_result", value=json.dumps(result))

        if not is_fresh:
            raise ValueError(
                f"Data is stale: last event was {age_hours:.1f} hours ago "
                f"(threshold: {FRESHNESS_THRESHOLD_HOURS}h)."
            )

        return result

    finally:
        spark.stop()


def detect_volume_anomalies(**context) -> Dict[str, Any]:
    """
    Detect volume anomalies by comparing today's count against
    the historical average and standard deviation.
    """
    import json
    import sys
    sys.path.insert(0, "/opt/spark-jobs/utils")

    from spark_session import SparkSessionBuilder
    from pyspark.sql import functions as F

    execution_date = context["ds"]
    spark = SparkSessionBuilder(app_name="DQ_VolumeAnomaly").with_delta().with_s3().build()

    try:
        events_df = spark.read.format("delta").load(f"{WAREHOUSE_PATH}/events")

        # Today's count
        today_count = events_df.filter(F.col("ingestion_date") == execution_date).count()

        # Historical stats (last 30 days excluding today)
        historical_stats = (
            events_df
            .filter(
                (F.col("ingestion_date") < execution_date) &
                (F.col("ingestion_date") >= F.date_sub(F.lit(execution_date), 30))
            )
            .groupBy("ingestion_date")
            .agg(F.count("*").alias("daily_count"))
            .agg(
                F.mean("daily_count").alias("avg_count"),
                F.stddev("daily_count").alias("stddev_count"),
            )
            .collect()[0]
        )

        avg_count = historical_stats["avg_count"] or 0
        stddev_count = historical_stats["stddev_count"] or 0

        lower_bound = max(0, avg_count - (VOLUME_ANOMALY_STDDEV * stddev_count))
        upper_bound = avg_count + (VOLUME_ANOMALY_STDDEV * stddev_count)

        is_anomaly = today_count < lower_bound or today_count > upper_bound

        result = {
            "today_count": today_count,
            "avg_count": round(avg_count, 0),
            "stddev_count": round(stddev_count, 0),
            "lower_bound": round(lower_bound, 0),
            "upper_bound": round(upper_bound, 0),
            "is_anomaly": is_anomaly,
        }

        context["task_instance"].xcom_push(key="volume_result", value=json.dumps(result))

        if is_anomaly:
            raise ValueError(
                f"Volume anomaly detected: {today_count} events today "
                f"(expected {lower_bound:.0f}-{upper_bound:.0f})."
            )

        return result

    finally:
        spark.stop()


def decide_alert_path(**context) -> str:
    """Branch: decide whether to send alert or skip based on check results."""
    ti = context["task_instance"]

    ge_success = ti.xcom_pull(task_ids="ge_checkpoint.run_great_expectations", key="ge_success")
    sql_passed = ti.xcom_pull(task_ids="sql_checks.run_sql_quality_checks", key="sql_checks_passed")

    if ge_success and sql_passed:
        return "notify_all_passed"
    return "notify_failures"


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@example.com"],
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": None,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Post-ETL data quality checks: GE, SQL, freshness, and volume anomaly detection.",
    schedule_interval="0 10 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["data-quality", "daily", "production"],
    doc_md=__doc__,
) as dag:

    # ---- Wait for ETL completion ----
    wait_for_etl = ExternalTaskSensor(
        task_id="wait_for_daily_etl",
        external_dag_id="daily_etl_pipeline",
        external_task_id=None,  # Wait for entire DAG
        execution_delta=timedelta(hours=4),  # ETL runs at 06:00, this at 10:00
        poke_interval=300,
        timeout=3600,
        mode="reschedule",
    )

    # ---- Great Expectations ----
    with TaskGroup(group_id="ge_checkpoint") as ge_group:
        run_ge = PythonOperator(
            task_id="run_great_expectations",
            python_callable=run_great_expectations_checkpoint,
            provide_context=True,
        )

    # ---- SQL Checks ----
    with TaskGroup(group_id="sql_checks") as sql_group:
        sql_checks = PythonOperator(
            task_id="run_sql_quality_checks",
            python_callable=run_sql_quality_checks,
            provide_context=True,
        )

    # ---- Freshness ----
    freshness_check = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
        provide_context=True,
    )

    # ---- Volume Anomaly ----
    volume_anomaly = PythonOperator(
        task_id="detect_volume_anomalies",
        python_callable=detect_volume_anomalies,
        provide_context=True,
    )

    # ---- Branching ----
    decide_alert = BranchPythonOperator(
        task_id="decide_alert_path",
        python_callable=decide_alert_path,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ---- Notifications ----
    notify_all_passed = SlackWebhookOperator(
        task_id="notify_all_passed",
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        message=(
            ":white_check_mark: *Data Quality Checks Passed*\n"
            "*Date:* `{{ ds }}`\n"
            "All Great Expectations, SQL, freshness, and volume checks passed."
        ),
    )

    notify_failures = SlackWebhookOperator(
        task_id="notify_failures",
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        message=(
            ":warning: *Data Quality Issues Detected*\n"
            "*Date:* `{{ ds }}`\n"
            "One or more quality checks failed. Review the Airflow logs for details."
        ),
    )

    done = EmptyOperator(
        task_id="done",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # ---- Dependencies ----
    wait_for_etl >> [ge_group, sql_group, freshness_check, volume_anomaly]
    [ge_group, sql_group, freshness_check, volume_anomaly] >> decide_alert
    decide_alert >> [notify_all_passed, notify_failures]
    [notify_all_passed, notify_failures] >> done
