"""
Airflow DAG for Data Quality Monitoring
Scheduled data quality checks and validation
"""

import sys
import os
import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, cast

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from airflow import DAG  
from airflow.operators.python import PythonOperator  
from airflow.operators.email import EmailOperator  
from airflow.utils.dates import days_ago  
from airflow.models import Variable  
from airflow.models.taskinstance import TaskInstance  
import structlog

logger = structlog.get_logger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'feature_store_data_quality',
    default_args=default_args,
    description='Data quality monitoring and validation',
    schedule_interval='0 */4 * * *',  # type: ignore
    tags=['feature-store', 'data-quality', 'monitoring'],
    max_active_runs=1
)


def run_data_quality_checks(**context: Any) -> Dict[str, Any]:
    """Execute comprehensive data quality checks"""
    try:
        from src.pipelines.data_quality import data_quality_pipeline  # type: ignore

        logger.info("Starting data quality checks")

        results = asyncio.run(data_quality_pipeline.run_quality_checks())

        # Cast context for type checking
        ti = cast(TaskInstance, context['task_instance'])
        ti.xcom_push(key='quality_results', value=results)

        logger.info(
            "Data quality checks completed",
            status=results.get('status'),
            failed_checks=len(results.get('alerts', [])),
            success_rate=results.get('summary', {}).get('success_rate', 0)
        )

        return results

    except Exception as e:
        logger.error(f"Data quality checks failed: {e}")
        raise


def process_quality_alerts(**context: Any) -> Dict[str, Any]:
    """Process and categorize quality alerts"""
    try:
        ti = cast(TaskInstance, context['task_instance'])
        results = ti.xcom_pull(key='quality_results') or {}
        alerts = results.get('alerts', [])

        if not alerts:
            logger.info("No data quality alerts found")
            return {'alert_count': 0, 'critical_alerts': []}

        critical_alerts = [a for a in alerts if a.get('severity') == 'error']
        warning_alerts = [a for a in alerts if a.get('severity') == 'warning']

        ti.xcom_push(key='critical_alerts', value=critical_alerts)
        ti.xcom_push(key='warning_alerts', value=warning_alerts)

        logger.info(
            "Processed quality alerts",
            total_alerts=len(alerts),
            critical_alerts=len(critical_alerts),
            warning_alerts=len(warning_alerts)
        )

        return {
            'alert_count': len(alerts),
            'critical_alerts': critical_alerts,
            'warning_alerts': warning_alerts
        }

    except Exception as e:
        logger.error(f"Failed to process quality alerts: {e}")
        raise


def update_quality_metrics(**context: Any) -> bool:
    """Update data quality metrics in Airflow variables"""
    try:
        ti = cast(TaskInstance, context['task_instance'])
        results = ti.xcom_pull(key='quality_results') or {}
        alert_summary = ti.xcom_pull(key='return_value', task_ids='process_alerts') or {}

        Variable.set("last_quality_check", datetime.utcnow().isoformat())
        Variable.set("quality_check_status", results.get('status', 'unknown'))
        Variable.set("quality_success_rate", results.get('summary', {}).get('success_rate', 0))
        Variable.set("critical_alerts_count", len(alert_summary.get('critical_alerts', [])))

        logger.info("Quality metrics updated")
        return True

    except Exception as e:
        logger.error(f"Failed to update quality metrics: {e}")
        return False


def prepare_alert_email(**context: Any) -> Dict[str, Any]:
    """Prepare email content for critical alerts"""
    try:
        ti = cast(TaskInstance, context['task_instance'])
        critical_alerts = ti.xcom_pull(key='critical_alerts') or []

        if not critical_alerts:
            return {'email_required': False}

        subject = f"[CRITICAL] Feature Store Data Quality Alerts - {len(critical_alerts)} Issues"
        body = "Critical data quality issues detected in Feature Store:\n\n"

        for i, alert in enumerate(critical_alerts, 1):
            body += f"{i}. {alert.get('type', 'Unknown')}\n"
            body += f"   Table: {alert.get('table', 'N/A')}\n"
            body += f"   Message: {alert.get('message', 'N/A')}\n"
            body += f"   Time: {alert.get('timestamp', 'N/A')}\n\n"

        body += "Please investigate and resolve these issues promptly.\n"
        body += "Full report available in Airflow task logs.\n"

        ti.xcom_push(key='email_subject', value=subject)
        ti.xcom_push(key='email_body', value=body)

        return {'email_required': True, 'alert_count': len(critical_alerts)}

    except Exception as e:
        logger.error(f"Failed to prepare alert email: {e}")
        return {'email_required': False, 'error': str(e)}


def create_quality_report(**context: Any) -> Dict[str, Any]:
    """Create detailed quality report"""
    try:
        ti = cast(TaskInstance, context['task_instance'])
        results = ti.xcom_pull(key='quality_results') or {}

        report = {
            'timestamp': results.get('timestamp'),
            'status': results.get('status'),
            'summary': results.get('summary', {}),
            'checks_details': results.get('checks', {}),
            'alerts': results.get('alerts', [])
        }

        report_key = f"quality_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        Variable.set(report_key, str(report))

        logger.info("Quality report created", report_key=report_key)
        return {'report_key': report_key}

    except Exception as e:
        logger.error(f"Failed to create quality report: {e}")
        return {'error': str(e)}


# Define tasks
quality_checks_task = PythonOperator(
    task_id='run_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag,
)

process_alerts_task = PythonOperator(
    task_id='process_alerts',
    python_callable=process_quality_alerts,
    dag=dag,
)

update_metrics_task = PythonOperator(
    task_id='update_metrics',
    python_callable=update_quality_metrics,
    dag=dag,
)

prepare_email_task = PythonOperator(
    task_id='prepare_alert_email',
    python_callable=prepare_alert_email,
    dag=dag,
)

send_alert_email = EmailOperator(
    task_id='send_critical_alerts',
    to=['data-engineering@company.com', 'ops@company.com'],
    subject="{{ task_instance.xcom_pull(key='email_subject') }}",
    html_content="{{ task_instance.xcom_pull(key='email_body') }}",
    dag=dag,
)

create_report_task = PythonOperator(
    task_id='create_quality_report',
    python_callable=create_quality_report,
    dag=dag,
    trigger_rule='all_done',
)

# Task dependencies
quality_checks_task >> process_alerts_task >> [update_metrics_task, prepare_email_task] >> create_report_task # type: ignore
prepare_email_task >> send_alert_email # type: ignore
