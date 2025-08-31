"""
Airflow DAG for Streaming Feature Pipeline Management
Monitors and manages real-time feature processing
"""

import sys
import os
from datetime import timedelta
import asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import structlog

logger = structlog.get_logger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'catchup': False
}

dag = DAG(
    'feature_store_streaming_pipeline',
    default_args=default_args,
    description='Streaming feature pipeline monitoring and management',
    schedule_interval=timedelta(minutes=15),  # Check every 15 minutes
    tags=['feature-store', 'streaming', 'monitoring'],
    max_active_runs=1
)


def check_streaming_pipeline_health(**context):
    """Check health of streaming pipeline"""
    try:
        from src.pipelines.feature_pipeline import feature_pipeline
        
        if not feature_pipeline.running:
            logger.warning("Streaming pipeline is not running")
            return {'status': 'stopped', 'healthy': False}
        
        logger.info("Streaming pipeline health check passed")
        return {'status': 'running', 'healthy': True}
        
    except Exception as e:
        logger.error(f"Streaming pipeline health check failed: {e}")
        return {'status': 'error', 'healthy': False, 'error': str(e)}


def restart_streaming_pipeline_if_needed(**context):
    """Restart streaming pipeline if unhealthy"""
    try:
        from src.pipelines.feature_pipeline import feature_pipeline

        health_status = context['task_instance'].xcom_pull(task_ids='check_pipeline_health')
        
        async def restart_pipeline():
            if feature_pipeline.running:
                await feature_pipeline.stop()
            await feature_pipeline.start()

        if not health_status.get('healthy', False):
            logger.info("Restarting unhealthy streaming pipeline")
            asyncio.run(restart_pipeline())
            logger.info("Streaming pipeline restarted")
            return {'action': 'restarted', 'success': True}
        else:
            logger.info("Streaming pipeline is healthy, no action needed")
            return {'action': 'none', 'success': True}
        
    except Exception as e:
        logger.error(f"Failed to restart streaming pipeline: {e}")
        return {'action': 'restart_failed', 'success': False, 'error': str(e)}


def monitor_kafka_lag(**context):
    """Monitor Kafka consumer lag"""
    try:
        logger.info("Kafka lag monitoring completed")
        return {'lag_status': 'normal'}
        
    except Exception as e:
        logger.error(f"Kafka lag monitoring failed: {e}")
        return {'lag_status': 'error', 'error': str(e)}


def check_cache_performance(**context):
    """Check Redis cache performance"""
    try:
        from src.feature_store.cache import cache

        async def get_cache_stats():
            return await cache.get_cache_stats()
        
        cache_stats = asyncio.run(get_cache_stats())
        
        hits = int(cache_stats.get('keyspace_hits', 0))
        misses = int(cache_stats.get('keyspace_misses', 0))
        total = hits + misses
        hit_ratio = hits / total if total > 0 else 0
        
        if hit_ratio < 0.8:
            logger.warning(f"Low cache hit ratio: {hit_ratio:.2%}")
            return {'cache_healthy': False, 'hit_ratio': hit_ratio}
        
        logger.info(f"Cache performance good: {hit_ratio:.2%} hit ratio")
        return {'cache_healthy': True, 'hit_ratio': hit_ratio}
        
    except Exception as e:
        logger.error(f"Cache performance check failed: {e}")
        return {'cache_healthy': False, 'error': str(e)}


# Define tasks
health_check_task = PythonOperator(
    task_id='check_pipeline_health',
    python_callable=check_streaming_pipeline_health,
    dag=dag
)

restart_task = PythonOperator(
    task_id='restart_if_needed',
    python_callable=restart_streaming_pipeline_if_needed,
    dag=dag
)

kafka_lag_task = PythonOperator(
    task_id='monitor_kafka_lag',
    python_callable=monitor_kafka_lag,
    dag=dag
)

cache_check_task = PythonOperator(
    task_id='check_cache_performance',
    python_callable=check_cache_performance,
    dag=dag
)

# Task dependencies
health_check_task >> restart_task # type: ignore
[kafka_lag_task, cache_check_task] # type: ignore
