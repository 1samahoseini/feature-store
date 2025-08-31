"""
Airflow DAG for Database Migration Management
PostgreSQL to CockroachDB migration orchestration
"""

import sys
import os
from datetime import timedelta
import asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
import structlog

logger = structlog.get_logger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': True,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,  # No automatic retries for migration
    'max_active_runs': 1,
    'catchup': False
}

dag = DAG(
    'feature_store_database_migration',
    default_args=default_args,
    description='PostgreSQL to CockroachDB migration pipeline',
    schedule_interval=None,  # Manual trigger only
    tags=['feature-store', 'migration', 'database'],
    max_active_runs=1
)


def validate_migration_prerequisites(**context):
    """Validate prerequisites before starting migration"""
    from src.pipelines.migration import migration

    async def run_validation():
        crdb_url = Variable.get("cockroachdb_connection_url")
        await migration.init_connections(crdb_url)
        try:
            validation_results = await migration.validate_migration_prerequisites()
            if not validation_results.get('ready_for_migration', False):
                error_msg = "Migration prerequisites not met"
                logger.error(error_msg, validation_results=validation_results)
                raise Exception(error_msg)
            logger.info("Migration prerequisites validated successfully")
            return validation_results
        finally:
            await migration.close_connections()

    try:
        result = asyncio.run(run_validation())
        context['task_instance'].xcom_push(key='validation_results', value=result)
        return result
    except Exception as e:
        logger.error(f"Migration validation failed: {e}")
        raise


def run_database_migration(**context):
    """Execute the actual database migration"""
    from src.pipelines.migration import migration

    async def run_migration_task():
        crdb_url = Variable.get("cockroachdb_connection_url")
        await migration.init_connections(crdb_url)
        try:
            migration_config = {
                'batch_size': int(Variable.get("migration_batch_size", "1000")),
                'auto_switchover': Variable.get("migration_auto_switchover", "false").lower() == "true",
                'parallel_tables': Variable.get("migration_parallel_tables", "false").lower() == "true",
                'validation_enabled': True
            }
            migration_results = await migration.run_migration(migration_config)
            if migration_results.get('status') != 'completed':
                error_msg = f"Migration failed: {migration_results.get('error', 'Unknown error')}"
                logger.error(error_msg)
                raise Exception(error_msg)
            logger.info(
                "Database migration completed successfully",
                migration_id=migration_results.get('migration_id'),
                duration_seconds=migration_results.get('duration_seconds')
            )
            return migration_results
        finally:
            await migration.close_connections()

    try:
        result = asyncio.run(run_migration_task())
        context['task_instance'].xcom_push(key='migration_results', value=result)
        return result
    except Exception as e:
        logger.error(f"Database migration failed: {e}")
        raise


# === Stub functions to avoid undefined errors ===
def post_migration_validation(**context):
    """Stub for post-migration validation"""
    logger.info("Post-migration validation placeholder executed")
    return True
def update_migration_status(**context):
    """Stub for updating migration status"""
    logger.info("Update migration status placeholder executed")
    return True


def prepare_rollback_plan(**context):
    """Stub for preparing rollback plan"""
    logger.info("Prepare rollback plan placeholder executed")
    return True


def cleanup_migration_artifacts(**context):
    """Stub for cleaning up migration artifacts"""
    logger.info("Cleanup migration artifacts placeholder executed")
    return True


# === Define tasks ===
start_task = BashOperator(
    task_id='start_migration',
    bash_command='echo "Starting database migration process"',
    dag=dag
)

validation_task = PythonOperator(
    task_id='validate_prerequisites',
    python_callable=validate_migration_prerequisites,
    dag=dag,
    pool='migration',
    pool_slots=1
)

backup_task = BashOperator(
    task_id='create_backup',
    bash_command='''...''',  
    dag=dag
)

migration_task = PythonOperator(
    task_id='run_migration',
    python_callable=run_database_migration,
    dag=dag,
    pool='migration',
    pool_slots=1,
    execution_timeout=timedelta(hours=4)
)

post_validation_task = PythonOperator(
    task_id='post_migration_validation',
    python_callable=post_migration_validation,
    dag=dag
)

status_update_task = PythonOperator(
    task_id='update_status',
    python_callable=update_migration_status,
    dag=dag,
    trigger_rule='all_done'
)

rollback_plan_task = PythonOperator(
    task_id='prepare_rollback_plan',
    python_callable=prepare_rollback_plan,
    dag=dag,
    trigger_rule='all_done'
)

cleanup_task = PythonOperator(
    task_id='cleanup_artifacts',
    python_callable=cleanup_migration_artifacts,
    dag=dag,
    trigger_rule='all_done'
)

end_task = BashOperator(
    task_id='end_migration',
    bash_command='echo "Database migration process completed"',
    dag=dag,
    trigger_rule='all_done'
)

# === Define task dependencies ===
start_task >> validation_task >> backup_task >> migration_task >> post_validation_task  # type: ignore
post_validation_task >> [status_update_task, rollback_plan_task] >> cleanup_task >> end_task  # type: ignore