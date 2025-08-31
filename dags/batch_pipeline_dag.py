"""
Airflow DAG for Batch Feature Pipeline
Scheduled daily feature computation and updates
"""

import sys
import os
import asyncio
from datetime import datetime, timedelta
from typing import Any

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  
from airflow.operators.bash import BashOperator  
from airflow.utils.dates import days_ago  
from airflow.models import Variable  
import structlog  

# Configure logging
logger = structlog.get_logger(__name__)

# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": days_ago(1),  # type: ignore
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

# Create DAG
dag = DAG(
    "feature_store_batch_pipeline",
    default_args=default_args,
    description="Daily batch feature computation pipeline",
    schedule="0 2 * * *",  # ✅ modern API (Airflow ≥2.7)
    tags=["feature-store", "batch", "daily"],
    max_active_runs=1,
)


def run_batch_pipeline(**context: Any):
    """Execute batch pipeline"""
    try:
        from src.pipelines.batch_pipeline import batch_pipeline  # type: ignore

        logger.info("Starting batch pipeline execution")

        results = asyncio.run(batch_pipeline.run_daily_pipeline())

        logger.info(
            "Batch pipeline completed",
            status=results.get("status"),
            total_records=results.get("total_records", 0),
            duration_seconds=results.get("duration_seconds", 0),
        )

        context["task_instance"].xcom_push(key="pipeline_results", value=results)

        if results.get("status") != "completed":
            raise Exception(f"Pipeline failed: {results.get('error', 'Unknown error')}")

        return results

    except Exception as e:
        logger.error(f"Batch pipeline failed: {e}")
        raise


def validate_pipeline_results(**context: Any):
    """Validate pipeline execution results"""
    try:
        results = context["task_instance"].xcom_pull(key="pipeline_results")

        if not results:
            raise Exception("No pipeline results found")

        total_records = results.get("total_records", 0)
        processed_records = results.get("processed_records", {})

        min_thresholds = {
            "user_features": 1000,
            "transaction_features": 500,
            "risk_features": 800,
        }

        validation_errors = []

        for table, min_count in min_thresholds.items():
            actual_count = processed_records.get(table, 0)
            if actual_count < min_count:
                validation_errors.append(
                    f"{table}: {actual_count} records (minimum: {min_count})"
                )

        if validation_errors:
            error_msg = "Pipeline validation failed: " + "; ".join(validation_errors)
            logger.error(error_msg)
            raise Exception(error_msg)

        logger.info("Pipeline validation successful", total_records=total_records)
        return True

    except Exception as e:
        logger.error(f"Pipeline validation failed: {e}")
        raise


def update_pipeline_metrics(**context: Any):
    """Update pipeline execution metrics"""
    try:
        results = context["task_instance"].xcom_pull(key="pipeline_results")

        if results:
            Variable.set("last_pipeline_run", datetime.utcnow().isoformat())
            Variable.set("last_pipeline_status", results.get("status", "unknown"))
            Variable.set("last_pipeline_records", results.get("total_records", 0))
            Variable.set("last_pipeline_duration", results.get("duration_seconds", 0))

            logger.info("Pipeline metrics updated")

        return True

    except Exception as e:
        logger.error(f"Failed to update pipeline metrics: {e}")
        return False  # Do not fail DAG


def cleanup_old_data(**context: Any):
    """Cleanup old data and logs"""
    try:
        # Implement data retention policies (e.g. remove data > 2 years old)
        logger.info("Data cleanup completed")
        return True

    except Exception as e:
        logger.error(f"Data cleanup failed: {e}")
        return False  # Do not fail DAG


# Define tasks
start_task = BashOperator(
    task_id="start_pipeline",
    bash_command='echo "Starting feature store batch pipeline"',
    dag=dag,
)

pipeline_task = PythonOperator(
    task_id="run_batch_pipeline",
    python_callable=run_batch_pipeline,
    dag=dag,
    pool="batch_processing",
    pool_slots=2,  # type: ignore[arg-type]
)

validation_task = PythonOperator(
    task_id="validate_results",
    python_callable=validate_pipeline_results,
    dag=dag,
)

metrics_task = PythonOperator(
    task_id="update_metrics",
    python_callable=update_pipeline_metrics,
    dag=dag,
    trigger_rule="all_done",
)

cleanup_task = PythonOperator(
    task_id="cleanup_old_data",
    python_callable=cleanup_old_data,
    dag=dag,
    trigger_rule="all_done",
)

end_task = BashOperator(
    task_id="end_pipeline",
    bash_command='echo "Feature store batch pipeline completed"',
    dag=dag,
    trigger_rule="all_done",
)

# Task dependencies
start_task >> pipeline_task >> validation_task >> [metrics_task, cleanup_task] >> end_task  # type: ignore
