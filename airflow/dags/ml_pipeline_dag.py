"""
ML Pipeline DAG
===============

End-to-end ML pipeline DAG that orchestrates:

1. Feature engineering (reads from warehouse, builds feature store)
2. Model training (trains a model, logs metrics)
3. Model evaluation (compares against champion model)
4. Deployment decision (BranchPythonOperator)
5. Model registration (registers the model if promoted)
6. Notification (success or skip)

Schedule: Weekly on Sundays at 10:00 UTC
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

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
MODEL_REGISTRY_PATH = Variable.get(
    "model_registry_path", default_var="s3a://ml-models/registry"
)
FEATURE_STORE_PATH = Variable.get(
    "feature_store_path", default_var="s3a://ml-models/features"
)
PROMOTION_THRESHOLD = float(Variable.get("model_promotion_threshold", default_var="0.02"))


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def build_features(**context) -> Dict[str, Any]:
    """Read from the warehouse and build training features.

    Produces a feature matrix stored in the feature store path.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    execution_date = context["ds"]
    spark = SparkSession.builder.appName("ml_feature_engineering").getOrCreate()

    try:
        events = spark.read.format("delta").load(f"{WAREHOUSE_BASE}/events")
        users = spark.read.format("delta").load(f"{WAREHOUSE_BASE}/users")

        # User-level feature aggregation
        user_features = (
            events.groupBy("user_id")
            .agg(
                F.count("*").alias("total_events"),
                F.countDistinct("event_type").alias("distinct_event_types"),
                F.countDistinct("computed_session_id").alias("total_sessions"),
                F.avg("session_duration_seconds").alias("avg_session_duration"),
                F.max("event_ts").alias("last_event_at"),
                F.min("event_ts").alias("first_event_at"),
                F.countDistinct("event_date").alias("active_days"),
                F.countDistinct("device_category").alias("device_count"),
            )
        )

        # Days since first/last event
        user_features = user_features.withColumn(
            "tenure_days",
            F.datediff(F.col("last_event_at"), F.col("first_event_at")),
        ).withColumn(
            "recency_days",
            F.datediff(F.current_date(), F.col("last_event_at")),
        )

        # Join with user attributes
        features = user_features.join(
            users.select("user_id", "signup_date", "plan_type", "country"),
            on="user_id",
            how="left",
        )

        output_path = f"{FEATURE_STORE_PATH}/{execution_date}"
        features.write.mode("overwrite").format("parquet").save(output_path)

        feature_count = features.count()
        logger.info("Built %d feature rows, saved to %s", feature_count, output_path)

        result = {"feature_path": output_path, "feature_count": feature_count}
        context["task_instance"].xcom_push(key="feature_result", value=json.dumps(result))
        return result

    finally:
        spark.stop()


def train_model(**context) -> Dict[str, Any]:
    """Train a classification model on the feature set.

    Uses scikit-learn for simplicity; in production this would use
    MLflow + a distributed framework.
    """
    import numpy as np
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
    from sklearn.model_selection import train_test_split

    import pandas as pd

    feature_result = json.loads(
        context["task_instance"].xcom_pull(
            task_ids="feature_engineering.build_features", key="feature_result"
        )
    )
    feature_path = feature_result["feature_path"]

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("ml_train").getOrCreate()

    try:
        features_df = spark.read.format("parquet").load(feature_path)

        # Create a binary label (e.g., churn prediction)
        from pyspark.sql import functions as F

        labeled = features_df.withColumn(
            "churned",
            F.when(F.col("recency_days") > 30, 1).otherwise(0),
        )

        pdf = labeled.select(
            "total_events",
            "distinct_event_types",
            "total_sessions",
            "avg_session_duration",
            "active_days",
            "tenure_days",
            "recency_days",
            "device_count",
            "churned",
        ).toPandas()

    finally:
        spark.stop()

    # Handle missing values
    pdf = pdf.fillna(0)

    X = pdf.drop("churned", axis=1)
    y = pdf["churned"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = GradientBoostingClassifier(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.1,
        random_state=42,
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    metrics = {
        "accuracy": round(accuracy_score(y_test, y_pred), 4),
        "precision": round(precision_score(y_test, y_pred, zero_division=0), 4),
        "recall": round(recall_score(y_test, y_pred, zero_division=0), 4),
        "f1": round(f1_score(y_test, y_pred, zero_division=0), 4),
        "train_size": len(X_train),
        "test_size": len(X_test),
    }

    logger.info("Model metrics: %s", json.dumps(metrics))
    context["task_instance"].xcom_push(key="model_metrics", value=json.dumps(metrics))
    return metrics


def evaluate_model(**context) -> Dict[str, Any]:
    """Compare candidate model metrics against the current champion."""
    candidate_metrics = json.loads(
        context["task_instance"].xcom_pull(
            task_ids="training.train_model", key="model_metrics"
        )
    )

    # Load champion metrics (from a previous run or model registry)
    champion_metrics = {"f1": 0.0}  # default when no champion exists
    try:
        champion_raw = Variable.get("champion_model_metrics")
        if champion_raw:
            champion_metrics = json.loads(champion_raw)
    except Exception:
        logger.info("No champion model found — candidate will be promoted")

    candidate_f1 = candidate_metrics.get("f1", 0.0)
    champion_f1 = champion_metrics.get("f1", 0.0)
    improvement = candidate_f1 - champion_f1

    evaluation = {
        "candidate_f1": candidate_f1,
        "champion_f1": champion_f1,
        "improvement": round(improvement, 4),
        "promote": improvement >= PROMOTION_THRESHOLD,
    }

    logger.info("Evaluation: %s", json.dumps(evaluation))
    context["task_instance"].xcom_push(key="evaluation", value=json.dumps(evaluation))
    return evaluation


def decide_deployment(**context) -> str:
    """Branch: promote the model or skip deployment."""
    evaluation = json.loads(
        context["task_instance"].xcom_pull(
            task_ids="evaluation.evaluate_model", key="evaluation"
        )
    )

    if evaluation.get("promote", False):
        logger.info("Model promoted — F1 improvement of %.4f", evaluation["improvement"])
        return "deployment.register_model"
    else:
        logger.info("Model not promoted — improvement %.4f below threshold", evaluation["improvement"])
        return "notify_skip"


def register_model(**context) -> Dict[str, Any]:
    """Register the candidate model in the model registry."""
    execution_date = context["ds"]
    metrics = json.loads(
        context["task_instance"].xcom_pull(
            task_ids="training.train_model", key="model_metrics"
        )
    )

    model_version = f"v_{execution_date.replace('-', '')}"
    registry_entry = {
        "model_name": "churn_prediction",
        "version": model_version,
        "metrics": metrics,
        "registered_at": datetime.utcnow().isoformat(),
        "status": "champion",
    }

    # Persist champion metrics for future comparisons
    Variable.set("champion_model_metrics", json.dumps(metrics))

    logger.info("Model registered: %s", json.dumps(registry_entry))
    context["task_instance"].xcom_push(
        key="registry_entry", value=json.dumps(registry_entry)
    )

    return registry_entry


def notify_deployment(**context) -> None:
    """Notify about successful model deployment."""
    registry_raw = context["task_instance"].xcom_pull(
        task_ids="deployment.register_model", key="registry_entry"
    )
    registry = json.loads(registry_raw) if registry_raw else {}

    message = (
        f":rocket: *New Model Deployed*\n"
        f"*Model:* `{registry.get('model_name', 'unknown')}`\n"
        f"*Version:* `{registry.get('version', 'unknown')}`\n"
        f"*F1 Score:* {registry.get('metrics', {}).get('f1', 'N/A')}"
    )
    SlackWebhookOperator(
        task_id="slack_deploy",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=message,
    ).execute(context=context)


def notify_skip(**context) -> None:
    """Notify that model deployment was skipped."""
    execution_date = context["ds"]
    message = (
        f":fast_forward: *Model Deployment Skipped*\n"
        f"*Date:* {execution_date}\n"
        f"Candidate model did not exceed champion by the promotion threshold ({PROMOTION_THRESHOLD})."
    )
    SlackWebhookOperator(
        task_id="slack_skip",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=message,
    ).execute(context=context)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "ml-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email": ["ml-engineering@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "execution_timeout": timedelta(hours=4),
}

with DAG(
    dag_id="ml_pipeline",
    default_args=default_args,
    description="Weekly ML pipeline: feature engineering -> training -> evaluation -> deployment",
    schedule_interval="0 10 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "production", "weekly"],
    doc_md=__doc__,
) as dag:

    # -- Feature Engineering -----------------------------------------------
    with TaskGroup("feature_engineering") as feature_group:
        features = PythonOperator(
            task_id="build_features",
            python_callable=build_features,
            provide_context=True,
        )

    # -- Training ----------------------------------------------------------
    with TaskGroup("training") as training_group:
        train = PythonOperator(
            task_id="train_model",
            python_callable=train_model,
            provide_context=True,
        )

    # -- Evaluation --------------------------------------------------------
    with TaskGroup("evaluation") as eval_group:
        evaluate = PythonOperator(
            task_id="evaluate_model",
            python_callable=evaluate_model,
            provide_context=True,
        )

    # -- Deployment decision -----------------------------------------------
    branch = BranchPythonOperator(
        task_id="decide_deployment",
        python_callable=decide_deployment,
        provide_context=True,
    )

    # -- Deployment --------------------------------------------------------
    with TaskGroup("deployment") as deploy_group:
        register = PythonOperator(
            task_id="register_model",
            python_callable=register_model,
            provide_context=True,
        )

    # -- Notifications -----------------------------------------------------
    notify_deploy = PythonOperator(
        task_id="notify_deployment",
        python_callable=notify_deployment,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    skip_notify = PythonOperator(
        task_id="notify_skip",
        python_callable=notify_skip,
        provide_context=True,
    )

    # -- Dependencies ------------------------------------------------------
    feature_group >> training_group >> eval_group >> branch
    branch >> deploy_group >> notify_deploy
    branch >> skip_notify
