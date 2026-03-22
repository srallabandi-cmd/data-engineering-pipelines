"""
ML Pipeline DAG

Orchestrates the machine learning workflow:
1. Feature computation from warehouse data
2. Model training with hyperparameter tuning
3. Model evaluation against baseline metrics
4. Conditional deployment (only if new model outperforms baseline)
5. Trigger downstream prediction pipeline

Schedule: Weekly on Sundays at 02:00 UTC
"""

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DAG_ID = "ml_pipeline"
SLACK_WEBHOOK_CONN_ID = "slack_webhook"
WAREHOUSE_PATH = Variable.get("warehouse_path", default_var="s3a://data-lake/warehouse")
FEATURE_STORE_PATH = Variable.get("feature_store_path", default_var="s3a://data-lake/features")
MODEL_REGISTRY_PATH = Variable.get("model_registry_path", default_var="s3a://data-lake/models")
METRIC_IMPROVEMENT_THRESHOLD = float(
    Variable.get("ml_metric_improvement_threshold", default_var="0.01")
)


# ---------------------------------------------------------------------------
# Task Callables
# ---------------------------------------------------------------------------

def compute_features(**context) -> Dict[str, Any]:
    """
    Compute ML features from warehouse data.

    Generates feature vectors for user behavior prediction:
    - User engagement metrics (session counts, event counts, avg duration)
    - Recency features (days since last activity)
    - Revenue features (total spend, avg order value)
    - Temporal features (day of week, hour of day distributions)
    """
    import json
    import sys
    sys.path.insert(0, "/opt/spark-jobs/utils")

    from spark_session import SparkSessionBuilder
    from pyspark.sql import functions as F
    from pyspark.sql import Window

    execution_date = context["ds"]
    training_window_days = 90

    spark = SparkSessionBuilder(app_name="ML_FeatureComputation").with_delta().with_s3().build()

    try:
        events_df = spark.read.format("delta").load(f"{WAREHOUSE_PATH}/events")

        # Filter to training window
        events = events_df.filter(
            (F.col("ingestion_date") >= F.date_sub(F.lit(execution_date), training_window_days)) &
            (F.col("ingestion_date") <= execution_date)
        )

        # User-level features
        user_features = (
            events
            .groupBy("user_id")
            .agg(
                F.count("event_id").alias("total_events"),
                F.countDistinct("computed_session_id").alias("total_sessions"),
                F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
                F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("pageview_count"),
                F.sum("revenue").alias("total_revenue"),
                F.avg("revenue").alias("avg_revenue"),
                F.max("revenue").alias("max_revenue"),
                F.avg("duration_ms").alias("avg_duration_ms"),
                F.max("event_timestamp").alias("last_activity"),
                F.min("event_timestamp").alias("first_activity"),
                F.countDistinct("device_type").alias("device_count"),
                F.countDistinct("country").alias("country_count"),
                F.countDistinct(F.to_date("event_timestamp")).alias("active_days"),
            )
            .withColumn(
                "days_since_last_activity",
                F.datediff(F.lit(execution_date), F.col("last_activity")),
            )
            .withColumn(
                "account_age_days",
                F.datediff(F.col("last_activity"), F.col("first_activity")),
            )
            .withColumn(
                "events_per_session",
                F.when(F.col("total_sessions") > 0, F.col("total_events") / F.col("total_sessions")).otherwise(0),
            )
            .withColumn(
                "conversion_rate",
                F.when(F.col("total_events") > 0, F.col("purchase_count") / F.col("total_events")).otherwise(0),
            )
            .withColumn(
                "activity_frequency",
                F.when(F.col("account_age_days") > 0, F.col("active_days") / F.col("account_age_days")).otherwise(0),
            )
            # Label: churned if no activity in last 14 days
            .withColumn(
                "is_churned",
                F.when(F.col("days_since_last_activity") > 14, 1).otherwise(0),
            )
            .drop("last_activity", "first_activity")
        )

        # Write features
        output_path = f"{FEATURE_STORE_PATH}/user_features/execution_date={execution_date}"
        feature_count = user_features.count()

        user_features.coalesce(10).write.mode("overwrite").parquet(output_path)

        result = {
            "feature_count": feature_count,
            "feature_columns": len(user_features.columns),
            "output_path": output_path,
        }

        context["task_instance"].xcom_push(key="feature_result", value=json.dumps(result))
        context["task_instance"].xcom_push(key="feature_path", value=output_path)

        return result

    finally:
        spark.stop()


def train_model(**context) -> Dict[str, Any]:
    """
    Train a churn prediction model using PySpark MLlib.

    Uses gradient boosted trees with cross-validation.
    """
    import json
    import sys
    sys.path.insert(0, "/opt/spark-jobs/utils")

    from spark_session import SparkSessionBuilder
    from pyspark.sql import functions as F
    from pyspark.ml.feature import VectorAssembler, StandardScaler
    from pyspark.ml.classification import GBTClassifier
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
    from pyspark.ml import Pipeline

    execution_date = context["ds"]
    feature_path = context["task_instance"].xcom_pull(
        task_ids="feature_engineering.compute_features", key="feature_path",
    )

    spark = SparkSessionBuilder(app_name="ML_TrainModel").with_delta().with_s3().build()

    try:
        features_df = spark.read.parquet(feature_path).na.fill(0)

        feature_columns = [
            "total_events", "total_sessions", "purchase_count", "pageview_count",
            "total_revenue", "avg_revenue", "max_revenue", "avg_duration_ms",
            "device_count", "country_count", "active_days",
            "days_since_last_activity", "account_age_days",
            "events_per_session", "conversion_rate", "activity_frequency",
        ]

        assembler = VectorAssembler(inputCols=feature_columns, outputCol="raw_features")
        scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
        gbt = GBTClassifier(labelCol="is_churned", featuresCol="features", maxIter=100)

        pipeline = Pipeline(stages=[assembler, scaler, gbt])

        param_grid = (
            ParamGridBuilder()
            .addGrid(gbt.maxDepth, [5, 8, 12])
            .addGrid(gbt.stepSize, [0.05, 0.1])
            .addGrid(gbt.subsamplingRate, [0.7, 0.9])
            .build()
        )

        evaluator = BinaryClassificationEvaluator(
            labelCol="is_churned", metricName="areaUnderROC",
        )

        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3,
            parallelism=4,
        )

        train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)

        cv_model = cv.fit(train_df)

        # Evaluate on test set
        predictions = cv_model.transform(test_df)
        auc_roc = evaluator.evaluate(predictions)

        precision_evaluator = BinaryClassificationEvaluator(
            labelCol="is_churned", metricName="areaUnderPR",
        )
        auc_pr = precision_evaluator.evaluate(predictions)

        # Save model
        model_path = f"{MODEL_REGISTRY_PATH}/churn_model/execution_date={execution_date}"
        cv_model.bestModel.write().overwrite().save(model_path)

        result = {
            "auc_roc": round(auc_roc, 4),
            "auc_pr": round(auc_pr, 4),
            "model_path": model_path,
            "train_count": train_df.count(),
            "test_count": test_df.count(),
            "best_params": str(cv_model.bestModel.stages[-1].extractParamMap()),
        }

        context["task_instance"].xcom_push(key="training_result", value=json.dumps(result))
        context["task_instance"].xcom_push(key="model_path", value=model_path)
        context["task_instance"].xcom_push(key="auc_roc", value=auc_roc)

        return result

    finally:
        spark.stop()


def evaluate_model(**context) -> Dict[str, Any]:
    """
    Compare the new model against the baseline (previous production model).

    Loads the baseline metrics and compares AUC-ROC.
    """
    import json

    ti = context["task_instance"]
    new_auc = float(ti.xcom_pull(task_ids="model_training.train_model", key="auc_roc"))

    # Load baseline metrics (from Airflow Variable or previous run)
    baseline_auc = float(Variable.get("ml_baseline_auc_roc", default_var="0.5"))

    improvement = new_auc - baseline_auc
    meets_threshold = improvement >= METRIC_IMPROVEMENT_THRESHOLD

    result = {
        "new_auc_roc": round(new_auc, 4),
        "baseline_auc_roc": round(baseline_auc, 4),
        "improvement": round(improvement, 4),
        "threshold": METRIC_IMPROVEMENT_THRESHOLD,
        "should_deploy": meets_threshold,
    }

    ti.xcom_push(key="evaluation_result", value=json.dumps(result))
    ti.xcom_push(key="should_deploy", value=meets_threshold)

    return result


def decide_deployment(**context) -> str:
    """
    Branch: deploy model if it outperforms baseline, otherwise skip.
    """
    ti = context["task_instance"]
    should_deploy = ti.xcom_pull(
        task_ids="model_evaluation.evaluate_model", key="should_deploy",
    )

    if should_deploy:
        return "deployment.deploy_model"
    return "deployment.skip_deployment"


def deploy_model(**context) -> Dict[str, Any]:
    """
    Deploy the trained model to the serving layer.

    Updates the production model pointer and the baseline metrics.
    """
    import json

    ti = context["task_instance"]
    model_path = ti.xcom_pull(task_ids="model_training.train_model", key="model_path")
    new_auc = float(ti.xcom_pull(task_ids="model_training.train_model", key="auc_roc"))

    # Update production model pointer
    Variable.set("ml_production_model_path", model_path)
    Variable.set("ml_baseline_auc_roc", str(round(new_auc, 4)))

    result = {
        "model_path": model_path,
        "new_baseline_auc": round(new_auc, 4),
        "deployed_at": datetime.utcnow().isoformat(),
    }

    ti.xcom_push(key="deployment_result", value=json.dumps(result))
    return result


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "ml-engineering",
    "depends_on_past": False,
    "email": ["ml-alerts@example.com"],
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=6),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="ML pipeline: feature computation, training, evaluation, and conditional deployment.",
    schedule_interval="0 2 * * 0",  # Weekly on Sundays
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ml", "weekly", "production"],
    doc_md=__doc__,
) as dag:

    # ---- Feature Engineering ----
    with TaskGroup(group_id="feature_engineering") as feature_group:
        compute_features_task = PythonOperator(
            task_id="compute_features",
            python_callable=compute_features,
            provide_context=True,
        )

    # ---- Model Training ----
    with TaskGroup(group_id="model_training") as training_group:
        train_model_task = PythonOperator(
            task_id="train_model",
            python_callable=train_model,
            provide_context=True,
        )

    # ---- Model Evaluation ----
    with TaskGroup(group_id="model_evaluation") as evaluation_group:
        evaluate_model_task = PythonOperator(
            task_id="evaluate_model",
            python_callable=evaluate_model,
            provide_context=True,
        )

    # ---- Deployment Decision ----
    decide = BranchPythonOperator(
        task_id="decide_deployment",
        python_callable=decide_deployment,
        provide_context=True,
    )

    # ---- Deployment ----
    with TaskGroup(group_id="deployment") as deployment_group:
        deploy = PythonOperator(
            task_id="deploy_model",
            python_callable=deploy_model,
            provide_context=True,
        )

        skip = EmptyOperator(task_id="skip_deployment")

    # ---- Trigger Downstream ----
    trigger_prediction = TriggerDagRunOperator(
        task_id="trigger_prediction_pipeline",
        trigger_dag_id="batch_prediction_pipeline",
        execution_date="{{ ds }}",
        wait_for_completion=False,
        trigger_rule=TriggerRule.ONE_SUCCESS,
        conf={
            "model_path": "{{ task_instance.xcom_pull(task_ids='model_training.train_model', key='model_path') }}",
            "execution_date": "{{ ds }}",
        },
    )

    # ---- Notifications ----
    notify_completion = SlackWebhookOperator(
        task_id="notify_completion",
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        message=(
            ":brain: *ML Pipeline Completed*\n"
            "*Date:* `{{ ds }}`\n"
            "*New AUC-ROC:* `{{ task_instance.xcom_pull(task_ids='model_training.train_model', key='auc_roc') }}`\n"
            "*Deployed:* `{{ task_instance.xcom_pull(task_ids='model_evaluation.evaluate_model', key='should_deploy') }}`\n"
        ),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ---- Dependencies ----
    feature_group >> training_group >> evaluation_group >> decide
    decide >> [deploy, skip]
    [deploy, skip] >> trigger_prediction >> notify_completion
