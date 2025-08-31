"""
Data Quality Pipeline
Monitoring data freshness, completeness, and anomalies
"""

import time
from datetime import datetime
from typing import Dict, Any, List

import structlog
from prometheus_client import Gauge, Counter

from src.config.settings import get_settings
from src.feature_store.database import db  # type: ignore
from src.feature_store.cache import cache  # type: ignore

logger = structlog.get_logger(__name__)

# Metrics
DATA_FRESHNESS = Gauge(
    'feature_store_data_freshness_minutes',
    'Data freshness in minutes',
    ['table']
)

DATA_COMPLETENESS = Gauge(
    'feature_store_data_completeness_ratio',
    'Data completeness ratio',
    ['table', 'column']
)

DATA_ANOMALIES = Counter(
    'feature_store_data_anomalies_total',
    'Data anomalies detected',
    ['table', 'anomaly_type']
)


class DataQualityPipeline:
    """Data quality monitoring and validation"""

    def __init__(self) -> None:
        self.settings = get_settings()
        self.quality_thresholds: Dict[str, Any] = {
            'freshness_hours': 2,  # Data should be fresher than 2 hours
            'completeness_ratio': 0.95,  # 95% completeness required
            'anomaly_threshold': 3.0  # 3 standard deviations for anomaly detection
        }

    async def run_quality_checks(self) -> Dict[str, Any]:
        """Run comprehensive data quality checks"""
        start_time = time.time()

        logger.info("Starting data quality checks")

        try:
            results: Dict[str, Any] = {
                "timestamp": datetime.utcnow().isoformat(),
                "status": "running",
                "checks": {},
                "alerts": [],
                "summary": {}
            }

            # Check data freshness
            freshness_results = await self._check_data_freshness()
            results["checks"]["freshness"] = freshness_results

            # Check data completeness
            completeness_results = await self._check_data_completeness()
            results["checks"]["completeness"] = completeness_results

            # Check for anomalies
            anomaly_results = await self._check_data_anomalies()
            results["checks"]["anomalies"] = anomaly_results

            # Check cache health
            cache_results = await self._check_cache_health()
            results["checks"]["cache"] = cache_results

            # Generate alerts
            alerts = self._generate_quality_alerts(results["checks"])
            results["alerts"] = alerts

            # Summary
            # total_checks: sum of number of checks (for each top-level check that is a dict)
            total_checks = sum(
                len(check_results) if isinstance(check_results, dict) else 0
                for check_results in results["checks"].values()
            )
            failed_checks = len(alerts)

            results["summary"] = {
                "total_checks": total_checks,
                "failed_checks": failed_checks,
                "success_rate": (total_checks - failed_checks) / total_checks if total_checks > 0 else 1.0,
                "duration_seconds": time.time() - start_time
            }

            results["status"] = "completed" if failed_checks == 0 else "completed_with_issues"

            logger.info(
                "Data quality checks completed",
                total_checks=total_checks,
                failed_checks=failed_checks,
                success_rate=results["summary"]["success_rate"]
            )

            return results

        except Exception as e:
            logger.error(f"Data quality checks failed: {e}")
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "status": "error",
                "error": str(e),
                "duration_seconds": time.time() - start_time
            }

    async def _check_data_freshness(self) -> Dict[str, Any]:
        """Check data freshness for all feature tables"""
        freshness_results: Dict[str, Any] = {}
        tables = ['user_features', 'transaction_features', 'risk_features']

        try:
            pool = getattr(db, "pool", None)
            if not pool:
                # try to initialize db if it exposes init()
                init_fn = getattr(db, "init", None)
                if callable(init_fn):
                    await db.init()
                pool = getattr(db, "pool", None)

            if not pool:
                logger.warning("Database pool not available for freshness checks")
                return {"error": "Database connection not available"}

            async with pool.acquire() as conn:  # type: ignore[attr-defined]
                for table in tables:
                    query = f"""
                        SELECT 
                            MAX(updated_at) as latest_update,
                            COUNT(*) as total_records,
                            COUNT(*) FILTER (WHERE updated_at >= CURRENT_TIMESTAMP - INTERVAL '{int(self.quality_thresholds["freshness_hours"])} hours') as fresh_records
                        FROM {table}
                    """

                    row = await conn.fetchrow(query)

                    if row and row.get('latest_update'):
                        latest_update = row['latest_update']
                        # latest_update could be datetime with tz, ensure naive comparison
                        if hasattr(latest_update, "replace"):
                            latest_dt = latest_update.replace(tzinfo=None)
                        else:
                            latest_dt = latest_update

                        freshness_minutes = (datetime.utcnow() - latest_dt).total_seconds() / 60
                        total_records = int(row.get('total_records', 0) or 0)
                        fresh_records = int(row.get('fresh_records', 0) or 0)
                        fresh_ratio = fresh_records / total_records if total_records > 0 else 0.0

                        freshness_results[table] = {
                            "latest_update": latest_dt.isoformat() if hasattr(latest_dt, "isoformat") else str(latest_dt),
                            "freshness_minutes": freshness_minutes,
                            "total_records": total_records,
                            "fresh_records": fresh_records,
                            "fresh_ratio": fresh_ratio,
                            "is_fresh": freshness_minutes <= (float(self.quality_thresholds["freshness_hours"]) * 60.0)
                        }

                        # Update metrics
                        try:
                            DATA_FRESHNESS.labels(table=table).set(freshness_minutes)
                        except Exception:
                            # metrics update should not break pipeline
                            logger.debug("Failed to update DATA_FRESHNESS metric", table=table)

                    else:
                        freshness_results[table] = {
                            "error": "No data found",
                            "is_fresh": False
                        }

            return freshness_results

        except Exception as e:
            logger.error(f"Freshness check failed: {e}")
            return {"error": str(e)}

    async def _check_data_completeness(self) -> Dict[str, Any]:
        """Check data completeness for critical fields"""
        completeness_results: Dict[str, Any] = {}

        # Define critical fields for each table
        critical_fields: Dict[str, List[str]] = {
            'user_features': ['user_id', 'total_orders', 'avg_order_value'],
            'transaction_features': ['user_id', 'total_transactions_30d', 'total_amount_30d'],
            'risk_features': ['user_id', 'risk_score', 'credit_utilization_ratio']
        }

        try:
            pool = getattr(db, "pool", None)
            if not pool:
                logger.warning("Database pool not available for completeness checks")
                return {"error": "Database connection not available"}

            async with pool.acquire() as conn:  # type: ignore[attr-defined]
                for table, fields in critical_fields.items():
                    table_results: Dict[str, Any] = {}

                    # Get total record count
                    total_count_val = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                    total_count = int(total_count_val or 0)

                    if total_count == 0:
                        completeness_results[table] = {"error": "No records found"}
                        continue

                    for field in fields:
                        # Count non-null values
                        non_null_val = await conn.fetchval(
                            f"SELECT COUNT(*) FROM {table} WHERE {field} IS NOT NULL"
                        )
                        non_null_count = int(non_null_val or 0)

                        completeness_ratio = non_null_count / total_count if total_count > 0 else 0.0

                        table_results[field] = {
                            "total_records": total_count,
                            "non_null_records": non_null_count,
                            "completeness_ratio": completeness_ratio,
                            "is_complete": completeness_ratio >= float(self.quality_thresholds["completeness_ratio"])
                        }

                        # Update metrics
                        try:
                            DATA_COMPLETENESS.labels(table=table, column=field).set(completeness_ratio)
                        except Exception:
                            logger.debug("Failed to update DATA_COMPLETENESS metric", table=table, column=field)

                    completeness_results[table] = table_results

            return completeness_results

        except Exception as e:
            logger.error(f"Completeness check failed: {e}")
            return {"error": str(e)}

    async def _check_data_anomalies(self) -> Dict[str, Any]:
        """Check for statistical anomalies in numeric fields"""
        anomaly_results: Dict[str, Any] = {}

        # Define numeric fields to check
        numeric_fields: Dict[str, List[str]] = {
            'user_features': ['total_orders', 'avg_order_value', 'age'],
            'transaction_features': ['total_transactions_30d', 'total_amount_30d', 'avg_transaction_amount'],
            'risk_features': ['risk_score', 'credit_utilization_ratio', 'payment_delays_30d']
        }

        try:
            pool = getattr(db, "pool", None)
            if not pool:
                logger.warning("Database pool not available for anomaly checks")
                return {"error": "Database connection not available"}

            async with pool.acquire() as conn:  # type: ignore[attr-defined]
                for table, fields in numeric_fields.items():
                    table_results: Dict[str, Any] = {}

                    for field in fields:
                        # Calculate statistics
                        stats_query = f"""
                            SELECT 
                                AVG({field}::numeric) as mean,
                                STDDEV({field}::numeric) as stddev,
                                MIN({field}::numeric) as min_val,
                                MAX({field}::numeric) as max_val,
                                COUNT(*) as total_count,
                                COUNT(*) FILTER (WHERE {field}::numeric < (AVG({field}::numeric) - {float(self.quality_thresholds["anomaly_threshold"])} * STDDEV({field}::numeric)) 
                                                    OR {field}::numeric > (AVG({field}::numeric) + {float(self.quality_thresholds["anomaly_threshold"])} * STDDEV({field}::numeric))) as outlier_count
                            FROM {table} 
                            WHERE {field} IS NOT NULL AND updated_at >= CURRENT_DATE - INTERVAL '1 day'
                        """

                        row = await conn.fetchrow(stats_query)

                        total_count = int(row.get('total_count', 0) or 0) if row else 0
                        if row and total_count > 0:
                            mean_val = row.get('mean') or 0
                            stddev_val = row.get('stddev') or 0
                            min_val = row.get('min_val') or 0
                            max_val = row.get('max_val') or 0
                            outlier_count = int(row.get('outlier_count', 0) or 0)

                            outlier_ratio = outlier_count / total_count if total_count > 0 else 0.0

                            table_results[field] = {
                                "mean": float(mean_val),
                                "stddev": float(stddev_val),
                                "min_value": float(min_val),
                                "max_value": float(max_val),
                                "total_count": total_count,
                                "outlier_count": outlier_count,
                                "outlier_ratio": outlier_ratio,
                                "has_anomalies": outlier_ratio > 0.05  # More than 5% outliers is concerning
                            }

                            # Record anomalies
                            if outlier_ratio > 0.05:
                                try:
                                    DATA_ANOMALIES.labels(table=table, anomaly_type='statistical_outlier').inc()
                                except Exception:
                                    logger.debug("Failed to update DATA_ANOMALIES metric", table=table, field=field)
                        else:
                            table_results[field] = {"error": "Insufficient data for analysis"}

                    anomaly_results[table] = table_results

            return anomaly_results

        except Exception as e:
            logger.error(f"Anomaly check failed: {e}")
            return {"error": str(e)}

    async def _check_cache_health(self) -> Dict[str, Any]:
        """Check cache performance and health"""
        try:
            cache_stats = await cache.get_cache_stats()

            # Calculate cache hit ratio
            hits = int(cache_stats.get('keyspace_hits', 0) or 0)
            misses = int(cache_stats.get('keyspace_misses', 0) or 0)
            total_requests = hits + misses
            hit_ratio = hits / total_requests if total_requests > 0 else 0.0

            # Check if cache is healthy
            cache_healthy = await cache.health_check()

            return {
                "redis_connected": bool(cache_healthy),
                "hit_ratio": hit_ratio,
                "total_requests": total_requests,
                "memory_usage": cache_stats.get('used_memory', 'unknown'),
                "connected_clients": int(cache_stats.get('connected_clients', 0) or 0),
                "evicted_keys": int(cache_stats.get('evicted_keys', 0) or 0),
                "is_healthy": bool(cache_healthy) and (hit_ratio >= 0.8)  # 80% hit ratio threshold
            }

        except Exception as e:
            logger.error(f"Cache health check failed: {e}")
            return {"error": str(e), "is_healthy": False}

    def _generate_quality_alerts(self, check_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate alerts based on quality check results"""
        alerts: List[Dict[str, Any]] = []

        # Check freshness alerts
        freshness_checks = check_results.get('freshness', {}) or {}
        if isinstance(freshness_checks, dict):
            for table, result in freshness_checks.items():
                if isinstance(result, dict) and not result.get('is_fresh', True):
                    alerts.append({
                        "type": "data_freshness",
                        "severity": "warning",
                        "table": table,
                        "message": f"Data in {table} is stale ({float(result.get('freshness_minutes', 0)):.1f} minutes old)",
                        "timestamp": datetime.utcnow().isoformat()
                    })

        # Check completeness alerts
        completeness_checks = check_results.get('completeness', {}) or {}
        if isinstance(completeness_checks, dict):
            for table, fields in completeness_checks.items():
                if isinstance(fields, dict) and 'error' not in fields:
                    for field, result in fields.items():
                        if isinstance(result, dict) and not result.get('is_complete', True):
                            alerts.append({
                                "type": "data_completeness",
                                "severity": "error",
                                "table": table,
                                "field": field,
                                "message": f"Data completeness for {table}.{field} is {result.get('completeness_ratio', 0):.2%}",
                                "timestamp": datetime.utcnow().isoformat()
                            })

        # Check anomaly alerts
        anomaly_checks = check_results.get('anomalies', {}) or {}
        if isinstance(anomaly_checks, dict):
            for table, fields in anomaly_checks.items():
                if isinstance(fields, dict) and 'error' not in fields:
                    for field, result in fields.items():
                        if isinstance(result, dict) and result.get('has_anomalies', False):
                            alerts.append({
                                "type": "data_anomaly",
                                "severity": "warning",
                                "table": table,
                                "field": field,
                                "message": f"Detected {int(result.get('outlier_count', 0))} outliers in {table}.{field} ({result.get('outlier_ratio', 0):.2%})",
                                "timestamp": datetime.utcnow().isoformat()
                            })

        # Check cache alerts
        cache_checks = check_results.get('cache', {}) or {}
        if isinstance(cache_checks, dict) and not cache_checks.get('is_healthy', True):
            alerts.append({
                "type": "cache_health",
                "severity": "error",
                "message": f"Cache health issues detected - hit ratio: {cache_checks.get('hit_ratio', 0):.2%}",
                "timestamp": datetime.utcnow().isoformat()
            })

        return alerts

    async def run_data_validation(self, user_id: str, feature_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate feature data for a specific user"""
        validation_results: Dict[str, Any] = {
            "user_id": user_id,
            "timestamp": datetime.utcnow().isoformat(),
            "valid": True,
            "warnings": [],
            "errors": []
        }

        try:
            # Validate user features
            if 'user' in feature_data:
                user_validation = self._validate_user_features(feature_data['user'])
                validation_results["warnings"].extend(user_validation.get("warnings", []))
                validation_results["errors"].extend(user_validation.get("errors", []))

            # Validate transaction features
            if 'transaction' in feature_data:
                txn_validation = self._validate_transaction_features(feature_data['transaction'])
                validation_results["warnings"].extend(txn_validation.get("warnings", []))
                validation_results["errors"].extend(txn_validation.get("errors", []))

            # Validate risk features
            if 'risk' in feature_data:
                risk_validation = self._validate_risk_features(feature_data['risk'])
                validation_results["warnings"].extend(risk_validation.get("warnings", []))
                validation_results["errors"].extend(risk_validation.get("errors", []))

            validation_results["valid"] = len(validation_results["errors"]) == 0

            return validation_results

        except Exception as e:
            logger.error(f"Data validation failed for user {user_id}: {e}")
            return {
                "user_id": user_id,
                "valid": False,
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }

    def _validate_user_features(self, user_features: Dict[str, Any]) -> Dict[str, List[str]]:
        """Validate user features"""
        warnings: List[str] = []
        errors: List[str] = []

        # Age validation
        age = user_features.get('age')
        if age is not None:
            try:
                age_val = int(age)
                if age_val < 18 or age_val > 100:
                    warnings.append(f"Age {age_val} is outside normal range (18-100)")
            except Exception:
                warnings.append(f"Age value invalid: {age}")

        # Order validation
        total_orders = user_features.get('total_orders', 0)
        avg_order_value = user_features.get('avg_order_value', 0)

        try:
            total_orders_val = int(total_orders)
            avg_order_value_val = float(avg_order_value)
        except Exception:
            total_orders_val = total_orders if isinstance(total_orders, int) else 0
            avg_order_value_val = avg_order_value if isinstance(avg_order_value, (int, float)) else 0.0

        if total_orders_val < 0:
            errors.append("Total orders cannot be negative")

        if avg_order_value_val < 0:
            errors.append("Average order value cannot be negative")

        if total_orders_val > 0 and avg_order_value_val == 0:
            warnings.append("User has orders but zero average order value")

        return {"warnings": warnings, "errors": errors}

    def _validate_transaction_features(self, txn_features: Dict[str, Any]) -> Dict[str, List[str]]:
        """Validate transaction features"""
        warnings: List[str] = []
        errors: List[str] = []

        # Transaction count validation
        total_txns = txn_features.get('total_transactions_30d', 0)
        declined_txns = txn_features.get('transactions_declined_30d', 0)

        try:
            total_txns_val = int(total_txns)
            declined_txns_val = int(declined_txns)
        except Exception:
            total_txns_val = total_txns if isinstance(total_txns, int) else 0
            declined_txns_val = declined_txns if isinstance(declined_txns, int) else 0

        if total_txns_val < 0 or declined_txns_val < 0:
            errors.append("Transaction counts cannot be negative")

        if declined_txns_val > total_txns_val:
            errors.append("Declined transactions cannot exceed total transactions")

        # Amount validation
        total_amount = txn_features.get('total_amount_30d', 0)
        avg_amount = txn_features.get('avg_transaction_amount', 0)

        try:
            total_amount_val = float(total_amount)
            avg_amount_val = float(avg_amount)
        except Exception:
            total_amount_val = total_amount if isinstance(total_amount, (int, float)) else 0.0
            avg_amount_val = avg_amount if isinstance(avg_amount, (int, float)) else 0.0

        if total_amount_val < 0 or avg_amount_val < 0:
            errors.append("Transaction amounts cannot be negative")

        # Ratio validation
        weekend_ratio = txn_features.get('weekend_transaction_ratio', 0)
        night_ratio = txn_features.get('night_transaction_ratio', 0)

        try:
            weekend_ratio_val = float(weekend_ratio)
            night_ratio_val = float(night_ratio)
        except Exception:
            weekend_ratio_val = 0.0
            night_ratio_val = 0.0

        if not (0 <= weekend_ratio_val <= 1) or not (0 <= night_ratio_val <= 1):
            errors.append("Transaction ratios must be between 0 and 1")

        return {"warnings": warnings, "errors": errors}

    def _validate_risk_features(self, risk_features: Dict[str, Any]) -> Dict[str, List[str]]:
        """Validate risk features"""
        warnings: List[str] = []
        errors: List[str] = []

        # Risk score validation
        risk_score = risk_features.get('risk_score', 0)
        try:
            risk_score_val = float(risk_score)
        except Exception:
            risk_score_val = 0.0

        if not (0 <= risk_score_val <= 1):
            errors.append("Risk score must be between 0 and 1")

        # Credit utilization validation
        credit_util = risk_features.get('credit_utilization_ratio', 0)
        try:
            credit_util_val = float(credit_util)
        except Exception:
            credit_util_val = 0.0

        if not (0 <= credit_util_val <= 1):
            errors.append("Credit utilization ratio must be between 0 and 1")

        # Payment delay validation
        delays_30d = risk_features.get('payment_delays_30d', 0)
        delays_90d = risk_features.get('payment_delays_90d', 0)

        try:
            delays_30d_val = int(delays_30d)
            delays_90d_val = int(delays_90d)
        except Exception:
            delays_30d_val = delays_30d if isinstance(delays_30d, int) else 0
            delays_90d_val = delays_90d if isinstance(delays_90d, int) else 0

        if delays_30d_val < 0 or delays_90d_val < 0:
            errors.append("Payment delay counts cannot be negative")

        if delays_30d_val > delays_90d_val:
            warnings.append("30-day payment delays exceed 90-day delays (unusual pattern)")

        return {"warnings": warnings, "errors": errors}


# Global data quality pipeline instance
data_quality_pipeline = DataQualityPipeline()
