"""
Batch ETL Pipeline
Daily feature computation and BigQuery updates
"""

import time
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

import structlog
from google.cloud import bigquery
from prometheus_client import Counter, Histogram

from src.config.settings import get_settings
from src.feature_store.database import db  # type: ignore[attr-defined]

logger = structlog.get_logger(__name__)

# Metrics
PIPELINE_RUNS = Counter(
    'feature_store_pipeline_runs_total',
    'Pipeline runs',
    ['pipeline', 'status']
)

PIPELINE_DURATION = Histogram(
    'feature_store_pipeline_duration_seconds',
    'Pipeline duration',
    ['pipeline']
)

RECORDS_PROCESSED = Counter(
    'feature_store_records_processed_total',
    'Records processed',
    ['pipeline', 'table']
)


class BatchPipeline:
    """Batch feature computation pipeline"""

    def __init__(self):
        self.settings = get_settings()
        self.new_method()
        self.init_bigquery_client()

    def new_method(self):
        self.bigquery_client: Optional[bigquery.Client] = None

    def init_bigquery_client(self):
        """Initialize BigQuery client"""
        try:
            if self.settings.env == "gcp":
                self.bigquery_client = bigquery.Client(
                    project=self.settings.bigquery_project_id
                )
            else:
                logger.info("Using local BigQuery emulator")

        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")

    async def run_daily_pipeline(self) -> Dict[str, Any]:
        start_time = time.time()
        pipeline_name = "daily_batch"

        logger.info("Starting daily batch pipeline")

        try:
            results: Dict[str, Any] = {
                "pipeline": pipeline_name,
                "start_time": datetime.utcnow().isoformat(),
                "status": "running",
                "processed_records": {},
                "errors": []
            }

            # Step 1: Compute user features
            user_results = await self._compute_user_features()
            results["processed_records"]["user_features"] = user_results["count"]

            # Step 2: Compute transaction features
            txn_results = await self._compute_transaction_features()
            results["processed_records"]["transaction_features"] = txn_results["count"]

            # Step 3: Compute risk features
            risk_results = await self._compute_risk_features()
            results["processed_records"]["risk_features"] = risk_results["count"]

            # Step 4: Update BigQuery
            if self.bigquery_client:
                await self._update_bigquery_tables(results["processed_records"])

            duration = time.time() - start_time
            results.update({
                "status": "completed",
                "end_time": datetime.utcnow().isoformat(),
                "duration_seconds": duration,
                "total_records": sum(results["processed_records"].values())
            })

            PIPELINE_RUNS.labels(pipeline=pipeline_name, status="success").inc()
            PIPELINE_DURATION.labels(pipeline=pipeline_name).observe(duration)

            logger.info(
                "Daily batch pipeline completed successfully",
                duration_seconds=duration,
                total_records=results["total_records"]
            )

            return results

        except Exception as e:
            duration = time.time() - start_time
            PIPELINE_RUNS.labels(pipeline=pipeline_name, status="error").inc()

            logger.error(f"Daily batch pipeline failed: {e}", duration_seconds=duration)

            return {
                "pipeline": pipeline_name,
                "status": "failed",
                "error": str(e),
                "duration_seconds": duration,
                "end_time": datetime.utcnow().isoformat()
            }

    async def _compute_user_features(self) -> Dict[str, Any]:
        query = """
        WITH user_stats AS (
            SELECT 
                u.user_id,
                u.age,
                u.location_country,
                u.location_city,
                COUNT(o.order_id) as total_orders,
                AVG(o.amount) as avg_order_value,
                EXTRACT(DAY FROM CURRENT_DATE - MIN(o.created_at)) as days_since_first_order,
                MODE() WITHIN GROUP (ORDER BY o.payment_method) as preferred_payment_method,
                u.account_verified,
                CURRENT_TIMESTAMP as created_at,
                CURRENT_TIMESTAMP as updated_at
            FROM users u
            LEFT JOIN orders o ON u.user_id = o.user_id
            WHERE u.updated_at >= CURRENT_DATE - INTERVAL '1 day'
            GROUP BY u.user_id, u.age, u.location_country, u.location_city, u.account_verified
        )
        SELECT * FROM user_stats
        """
        return await self._execute_feature_computation(query, "user_features", "compute_user_features")

    async def _compute_transaction_features(self) -> Dict[str, Any]:
        query = """
        WITH transaction_stats AS (
            SELECT 
                o.user_id,
                COUNT(*) FILTER (WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days') as total_transactions_30d,
                SUM(o.amount) FILTER (WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days') as total_amount_30d,
                AVG(o.amount) FILTER (WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days') as avg_transaction_amount,
                MAX(o.amount) FILTER (WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days') as max_transaction_amount,
                COUNT(*) FILTER (WHERE o.status = 'declined' AND o.created_at >= CURRENT_DATE - INTERVAL '30 days') as transactions_declined_30d,
                COUNT(DISTINCT o.merchant_id) FILTER (WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days') as unique_merchants_30d,
                COUNT(*) FILTER (WHERE EXTRACT(DOW FROM o.created_at) IN (0, 6) AND o.created_at >= CURRENT_DATE - INTERVAL '30 days')::float / 
                    NULLIF(COUNT(*) FILTER (WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days'), 0) as weekend_transaction_ratio,
                COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM o.created_at) BETWEEN 22 AND 6 AND o.created_at >= CURRENT_DATE - INTERVAL '30 days')::float / 
                    NULLIF(COUNT(*) FILTER (WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days'), 0) as night_transaction_ratio,
                CURRENT_TIMESTAMP as created_at,
                CURRENT_TIMESTAMP as updated_at
            FROM orders o
            WHERE o.created_at >= CURRENT_DATE - INTERVAL '31 days'
            GROUP BY o.user_id
            HAVING COUNT(*) FILTER (WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days') > 0
        )
        SELECT * FROM transaction_stats
        """
        return await self._execute_feature_computation(query, "transaction_features", "compute_transaction_features")

    async def _compute_risk_features(self) -> Dict[str, Any]:
        query = """
        WITH risk_stats AS (
            SELECT 
                u.user_id,
                COALESCE(cr.credit_utilization_ratio, 0.0) as credit_utilization_ratio,
                COUNT(*) FILTER (WHERE p.payment_date > p.due_date AND p.payment_date >= CURRENT_DATE - INTERVAL '30 days') as payment_delays_30d,
                COUNT(*) FILTER (WHERE p.payment_date > p.due_date AND p.payment_date >= CURRENT_DATE - INTERVAL '90 days') as payment_delays_90d,
                COUNT(*) FILTER (WHERE p.status = 'failed' AND p.created_at >= CURRENT_DATE - INTERVAL '90 days') as failed_payments_count,
                COUNT(DISTINCT ul.device_id) FILTER (WHERE ul.created_at >= CURRENT_DATE - INTERVAL '30 days') as device_changes_30d,
                COUNT(DISTINCT ul.ip_address) FILTER (WHERE ul.created_at >= CURRENT_DATE - INTERVAL '30 days') as login_locations_30d,
                COUNT(*) FILTER (WHERE va.alert_type = 'velocity' AND va.created_at >= CURRENT_DATE - INTERVAL '30 days') as velocity_alerts_30d,
                COALESCE(rs.risk_score, 0.0) as risk_score,
                CURRENT_TIMESTAMP as created_at,
                CURRENT_TIMESTAMP as updated_at
            FROM users u
            LEFT JOIN payments p ON u.user_id = p.user_id
            LEFT JOIN user_logins ul ON u.user_id = ul.user_id
            LEFT JOIN velocity_alerts va ON u.user_id = va.user_id
            LEFT JOIN credit_reports cr ON u.user_id = cr.user_id
            LEFT JOIN risk_scores rs ON u.user_id = rs.user_id
            WHERE u.updated_at >= CURRENT_DATE - INTERVAL '1 day'
            GROUP BY u.user_id, cr.credit_utilization_ratio, rs.risk_score
        )
        SELECT * FROM risk_stats
        """
        return await self._execute_feature_computation(query, "risk_features", "compute_risk_features")

    async def _execute_feature_computation(self, query: str, table_name: str, operation: str) -> Dict[str, Any]:
        start_time = time.time()

        try:
            if not getattr(db, "pool", None):
                await db.init()

            if not db.pool:
                raise RuntimeError("Database connection pool is not initialized.")
            async with db.pool.acquire() as conn:
                rows = await conn.fetch(query)

                if not rows:
                    logger.warning(f"No data returned from {operation}")
                    return {"count": 0, "duration": time.time() - start_time}

                upsert_query = self._get_upsert_query(table_name)
                batch_size = 1000
                total_processed = 0
                batch_values: List[Tuple[Any, ...]]

                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i + batch_size]
                    batch_values = [tuple(dict(row).values()) for row in batch]
                    await conn.executemany(upsert_query, batch_values)
                    total_processed += len(batch)

                duration = time.time() - start_time
                RECORDS_PROCESSED.labels(pipeline="daily_batch", table=table_name).inc(total_processed)

                return {"count": total_processed, "duration": duration}

        except Exception as e:
            logger.error(f"Feature computation failed: {operation}", error=str(e))
            raise

    def _get_upsert_query(self, table_name: str) -> str:
        """
        Returns an upsert (insert or update) SQL query for the given table.
        This is a stub and should be replaced with the actual upsert query for your schema.
        """
        # Example for PostgreSQL ON CONFLICT upsert, adjust columns as needed
        if table_name == "user_features":
            return """
                INSERT INTO user_features (
                    user_id, age, location_country, location_city, total_orders, avg_order_value,
                    days_since_first_order, preferred_payment_method, account_verified, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (user_id) DO UPDATE SET
                    age = EXCLUDED.age,
                    location_country = EXCLUDED.location_country,
                    location_city = EXCLUDED.location_city,
                    total_orders = EXCLUDED.total_orders,
                    avg_order_value = EXCLUDED.avg_order_value,
                    days_since_first_order = EXCLUDED.days_since_first_order,
                    preferred_payment_method = EXCLUDED.preferred_payment_method,
                    account_verified = EXCLUDED.account_verified,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at
            """
        elif table_name == "transaction_features":
            return """
                INSERT INTO transaction_features (
                    user_id, total_transactions_30d, total_amount_30d, avg_transaction_amount,
                    max_transaction_amount, transactions_declined_30d, unique_merchants_30d,
                    weekend_transaction_ratio, night_transaction_ratio, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (user_id) DO UPDATE SET
                    total_transactions_30d = EXCLUDED.total_transactions_30d,
                    total_amount_30d = EXCLUDED.total_amount_30d,
                    avg_transaction_amount = EXCLUDED.avg_transaction_amount,
                    max_transaction_amount = EXCLUDED.max_transaction_amount,
                    transactions_declined_30d = EXCLUDED.transactions_declined_30d,
                    unique_merchants_30d = EXCLUDED.unique_merchants_30d,
                    weekend_transaction_ratio = EXCLUDED.weekend_transaction_ratio,
                    night_transaction_ratio = EXCLUDED.night_transaction_ratio,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at
            """
        elif table_name == "risk_features":
            return """
                INSERT INTO risk_features (
                    user_id, credit_utilization_ratio, payment_delays_30d, payment_delays_90d,
                    failed_payments_count, device_changes_30d, login_locations_30d,
                    velocity_alerts_30d, risk_score, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (user_id) DO UPDATE SET
                    credit_utilization_ratio = EXCLUDED.credit_utilization_ratio,
                    payment_delays_30d = EXCLUDED.payment_delays_30d,
                    payment_delays_90d = EXCLUDED.payment_delays_90d,
                    failed_payments_count = EXCLUDED.failed_payments_count,
                    device_changes_30d = EXCLUDED.device_changes_30d,
                    login_locations_30d = EXCLUDED.login_locations_30d,
                    velocity_alerts_30d = EXCLUDED.velocity_alerts_30d,
                    risk_score = EXCLUDED.risk_score,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at
            """
        else:
            raise ValueError(f"Unknown table for upsert: {table_name}")

    # The _get_upsert_query and BigQuery methods remain unchanged
    # Only minor typing fixes needed for _update_bigquery_tables
    async def _update_bigquery_tables(self, processed_records: Dict[str, int]):
        if not self.bigquery_client:
            logger.warning("BigQuery client not available, skipping warehouse update")
            return

        for table_name, record_count in processed_records.items():
            if record_count == 0:
                continue
            await self._export_to_bigquery(table_name, self.settings.bigquery_dataset)
            logger.info(f"Updated BigQuery table: {self.settings.bigquery_dataset}.{table_name}")

    async def _export_to_bigquery(self, table_name: str, dataset: str):
        """
        Export data from the local database to BigQuery.
        This is a stub implementation. Replace with actual export logic.
        """
        logger.info(f"Exporting {table_name} to BigQuery dataset {dataset}")
        # Example: You might use pandas-gbq, pyarrow, or BigQuery API here.
        # For now, just log the action.
        # TODO: Implement actual export logic.
        pass


# Global pipeline instance
batch_pipeline = BatchPipeline()
