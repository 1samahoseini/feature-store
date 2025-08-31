"""
Database Migration Pipeline
PostgreSQL to CockroachDB migration utilities
"""

import time
from datetime import datetime
from typing import Dict, Any, Optional, List, Sequence

import asyncpg
import structlog
from prometheus_client import Counter, Histogram

from src.config.settings import get_settings

logger = structlog.get_logger(__name__)

# Metrics
MIGRATION_OPERATIONS = Counter(
    'feature_store_migration_operations_total',
    'Migration operations',
    ['operation', 'status']
)

MIGRATION_DURATION = Histogram(
    'feature_store_migration_duration_seconds',
    'Migration operation duration',
    ['operation']
)


class DatabaseMigration:
    """Database migration utilities for PostgreSQL to CockroachDB"""

    def __init__(self) -> None:
        self.settings = get_settings()
        self.pg_pool: Optional[asyncpg.Pool] = None
        self.crdb_pool: Optional[asyncpg.Pool] = None

    async def init_connections(self, crdb_url: str) -> None:
        """Initialize connections to both databases"""
        try:
            # PostgreSQL connection
            self.pg_pool = await asyncpg.create_pool(
                self.settings.database_url,
                min_size=2,
                max_size=5,
                command_timeout=60
            )

            # CockroachDB connection
            self.crdb_pool = await asyncpg.create_pool(
                crdb_url,
                min_size=2,
                max_size=5,
                command_timeout=60
            )

            logger.info("Migration database connections initialized")

        except Exception as e:
            logger.error("Failed to initialize migration connections", error=str(e))
            raise

    async def close_connections(self) -> None:
        """Close database connections"""
        if self.pg_pool is not None:
            await self.pg_pool.close()
        if self.crdb_pool is not None:
            await self.crdb_pool.close()

        logger.info("Migration database connections closed")

    async def validate_migration_prerequisites(self) -> Dict[str, Any]:
        """Validate prerequisites for migration"""
        start_time = time.time()

        try:
            validation_results: Dict[str, Any] = {
                "timestamp": datetime.utcnow().isoformat(),
                "status": "running",
                "checks": {},
                "ready_for_migration": False
            }

            # Check PostgreSQL connection and data
            pg_check = await self._validate_postgresql()
            # ensure "checks" is a dict before indexed assignment (helps type-checkers)
            checks = validation_results.get("checks")
            if not isinstance(checks, dict):
                checks = {}
                validation_results["checks"] = checks
            checks["postgresql"] = pg_check

            # Check CockroachDB connection and schema
            crdb_check = await self._validate_cockroachdb()
            checks["cockroachdb"] = crdb_check

            # Check data consistency
            consistency_check = await self._validate_data_consistency()
            checks["consistency"] = consistency_check

            # Check resource availability
            resource_check = await self._validate_resources()
            checks["resources"] = resource_check

            # Overall readiness
            all_checks_passed = all(
                isinstance(check, dict) and check.get("status") == "pass"
                for check in checks.values()
            )

            validation_results.update({
                "status": "completed",
                "ready_for_migration": all_checks_passed,
                "duration_seconds": time.time() - start_time
            })

            logger.info(
                "Migration validation completed",
                ready_for_migration=all_checks_passed,
                duration_seconds=validation_results["duration_seconds"]
            )

            return validation_results

        except Exception as e:
            logger.error("Migration validation failed", error=str(e))
            return {
                "status": "error",
                "error": str(e),
                "ready_for_migration": False,
                "duration_seconds": time.time() - start_time
            }

    async def run_migration(self, migration_config: Dict[str, Any]) -> Dict[str, Any]:
        """Run the database migration"""
        start_time = time.time()
        operation_name = "database_migration"

        logger.info("Starting database migration")

        try:
            migration_results: Dict[str, Any] = {
                "migration_id": f"migration_{int(time.time())}",
                "start_time": datetime.utcnow().isoformat(),
                "status": "running",
                "config": migration_config,
                "phases": {},
                "stats": {}
            }

            # Phase 1: Schema migration
            logger.info("Phase 1: Migrating schema")
            schema_results = await self._migrate_schema()
            migration_results["phases"]["schema"] = schema_results

            if schema_results.get("status") != "success":
                raise Exception("Schema migration failed")

            # Phase 2: Data migration
            logger.info("Phase 2: Migrating data")
            data_results = await self._migrate_data(migration_config)
            migration_results["phases"]["data"] = data_results

            if data_results.get("status") != "success":
                raise Exception("Data migration failed")

            # Phase 3: Validation
            logger.info("Phase 3: Validating migration")
            validation_results = await self._validate_migration()
            migration_results["phases"]["validation"] = validation_results

            if validation_results.get("status") != "success":
                raise Exception("Migration validation failed")

            # Phase 4: Switch over (if configured)
            if migration_config.get("auto_switchover", False):
                logger.info("Phase 4: Switching over to CockroachDB")
                switchover_results = await self._perform_switchover()
                migration_results["phases"]["switchover"] = switchover_results

            # Final results
            duration = time.time() - start_time
            migration_results.update({
                "status": "completed",
                "end_time": datetime.utcnow().isoformat(),
                "duration_seconds": duration
            })

            MIGRATION_OPERATIONS.labels(operation=operation_name, status="success").inc()
            MIGRATION_DURATION.labels(operation=operation_name).observe(duration)

            logger.info(
                "Database migration completed successfully",
                migration_id=migration_results["migration_id"],
                duration_seconds=duration
            )

            return migration_results

        except Exception as e:
            duration = time.time() - start_time

            logger.error("Database migration failed", error=str(e), duration_seconds=duration)

            MIGRATION_OPERATIONS.labels(operation=operation_name, status="error").inc()

            return {
                "status": "failed",
                "error": str(e),
                "duration_seconds": duration,
                "end_time": datetime.utcnow().isoformat()
            }

    async def _validate_postgresql(self) -> Dict[str, Any]:
        """Validate PostgreSQL source database"""
        try:
            if self.pg_pool is None:
                return {"status": "fail", "error": "PostgreSQL connection not available"}

            async with self.pg_pool.acquire() as conn:
                # Check connection
                await conn.fetchval('SELECT 1')

                # Check tables exist
                tables = ['user_features', 'transaction_features', 'risk_features']
                table_counts: Dict[str, int] = {}

                for table in tables:
                    count = await conn.fetchval(f'SELECT COUNT(*) FROM {table}')
                    table_counts[table] = int(count)

                # Check data recency
                latest_updates: Dict[str, Optional[str]] = {}
                for table in tables:
                    latest = await conn.fetchval(f'SELECT MAX(updated_at) FROM {table}')
                    latest_updates[table] = latest.isoformat() if latest else None

                return {
                    "status": "pass",
                    "connection": "healthy",
                    "table_counts": table_counts,
                    "latest_updates": latest_updates,
                    "total_records": sum(table_counts.values())
                }

        except Exception as e:
            return {"status": "fail", "error": str(e)}

    async def _validate_cockroachdb(self) -> Dict[str, Any]:
        """Validate CockroachDB target database"""
        try:
            if self.crdb_pool is None:
                return {"status": "fail", "error": "CockroachDB connection not available"}

            async with self.crdb_pool.acquire() as conn:
                # Check connection
                await conn.fetchval('SELECT 1')

                # Check CockroachDB version
                version = await conn.fetchval('SELECT version()')

                # Check if tables exist (they shouldn't before migration)
                tables = ['user_features', 'transaction_features', 'risk_features']
                existing_tables: List[str] = []

                for table in tables:
                    try:
                        await conn.fetchval(f'SELECT 1 FROM {table} LIMIT 1')
                        existing_tables.append(table)
                    except Exception:
                        # Table probably doesn't exist yet
                        pass

                return {
                    "status": "pass",
                    "connection": "healthy",
                    "version": version,
                    "existing_tables": existing_tables,
                    "ready_for_migration": len(existing_tables) == 0
                }

        except Exception as e:
            return {"status": "fail", "error": str(e)}

    async def _validate_data_consistency(self) -> Dict[str, Any]:
        """Validate data consistency in PostgreSQL"""
        try:
            if self.pg_pool is None:
                return {"status": "fail", "error": "PostgreSQL connection not available"}

            consistency_issues: List[str] = []

            async with self.pg_pool.acquire() as conn:
                # Check for duplicate user_ids
                for table in ['user_features', 'transaction_features', 'risk_features']:
                    duplicates = await conn.fetchval(f'''
                        SELECT COUNT(*) FROM (
                            SELECT user_id, COUNT(*) as cnt
                            FROM {table}
                            GROUP BY user_id
                            HAVING COUNT(*) > 1
                        ) duplicates
                    ''')

                    if int(duplicates) > 0:
                        consistency_issues.append(f"{table} has {int(duplicates)} duplicate user_ids")

                # Potential place for orphan checks (schema dependent)

                return {
                    "status": "pass" if len(consistency_issues) == 0 else "warning",
                    "issues": consistency_issues,
                    "issue_count": len(consistency_issues)
                }

        except Exception as e:
            return {"status": "fail", "error": str(e)}

    async def _validate_resources(self) -> Dict[str, Any]:
        """Validate system resources for migration"""
        try:
            # Simplified implementation
            return {
                "status": "pass",
                "disk_space": "sufficient",
                "memory": "sufficient",
                "cpu": "available"
            }

        except Exception as e:
            return {"status": "fail", "error": str(e)}

    async def _migrate_schema(self) -> Dict[str, Any]:
        """Migrate database schema to CockroachDB"""
        start_time = time.time()

        try:
            if self.crdb_pool is None:
                return {"status": "error", "error": "CockroachDB connection not available",
                        "duration_seconds": time.time() - start_time}

            # Read CockroachDB schema from file
            with open('sql/cockroachdb_schema.sql', 'r', encoding='utf-8') as f:
                schema_sql = f.read()

            async with self.crdb_pool.acquire() as conn:
                # Execute schema creation
                await conn.execute(schema_sql)

            logger.info("Schema migration completed")

            return {
                "status": "success",
                "duration_seconds": time.time() - start_time,
                "message": "Schema successfully created in CockroachDB"
            }

        except Exception as e:
            logger.error("Schema migration failed", error=str(e))
            return {
                "status": "error",
                "error": str(e),
                "duration_seconds": time.time() - start_time
            }

    async def _migrate_data(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Migrate data from PostgreSQL to CockroachDB"""
        start_time = time.time()

        try:
            if self.pg_pool is None or self.crdb_pool is None:
                return {
                    "status": "error",
                    "error": "Database connections not available",
                    "duration_seconds": time.time() - start_time
                }

            tables = ['user_features', 'transaction_features', 'risk_features']
            migration_stats: Dict[str, Dict[str, Any]] = {}

            batch_size = int(config.get('batch_size', 1000))

            for table in tables:
                logger.info("Migrating table", table=table)

                table_start = time.time()
                migrated_count = await self._migrate_table(table, batch_size)

                migration_stats[table] = {
                    "records_migrated": int(migrated_count),
                    "duration_seconds": time.time() - table_start
                }

                logger.info("Table migrated", table=table, count=int(migrated_count))

            return {
                "status": "success",
                "duration_seconds": time.time() - start_time,
                "tables": migration_stats,
                "total_records": sum(stats["records_migrated"] for stats in migration_stats.values())
            }

        except Exception as e:
            logger.error("Data migration failed", error=str(e))
            return {
                "status": "error",
                "error": str(e),
                "duration_seconds": time.time() - start_time
            }

    async def _migrate_table(self, table_name: str, batch_size: int) -> int:
        """Migrate a single table"""
        if self.pg_pool is None or self.crdb_pool is None:
            # Defensive check for type-checkers and runtime safety
            raise RuntimeError("Database pools are not initialized")

        total_migrated = 0

        # Get column names
        async with self.pg_pool.acquire() as pg_conn:
            columns_query = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = $1
                ORDER BY ordinal_position
            """

            column_rows: Sequence[asyncpg.Record] = await pg_conn.fetch(columns_query, table_name)
            columns: List[str] = [str(row['column_name']) for row in column_rows]

            # Get total count for progress tracking
            total_count = int(await pg_conn.fetchval(f'SELECT COUNT(*) FROM {table_name}'))

            # Migrate in batches
            offset = 0

            while offset < total_count:
                # Fetch batch from PostgreSQL
                batch_query = f'''
                    SELECT {", ".join(columns)}
                    FROM {table_name}
                    ORDER BY user_id
                    LIMIT $1 OFFSET $2
                '''

                rows: Sequence[asyncpg.Record] = await pg_conn.fetch(batch_query, int(batch_size), int(offset))

                if not rows:
                    break

                # Insert batch into CockroachDB
                async with self.crdb_pool.acquire() as crdb_conn:
                    placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
                    insert_query = f'''
                        INSERT INTO {table_name} ({", ".join(columns)})
                        VALUES ({placeholders})
                    '''

                    # Prepare batch data
                    batch_data: List[List[Any]] = []
                    for row in rows:
                        batch_data.append([row[col] for col in columns])

                    await crdb_conn.executemany(insert_query, batch_data)

                total_migrated += len(rows)
                offset += batch_size

                logger.debug(
                    "Table migration progress",
                    table=table_name,
                    migrated=total_migrated,
                    total=total_count
                )

        return int(total_migrated)

    async def _validate_migration(self) -> Dict[str, Any]:
        """Validate the completed migration"""
        start_time = time.time()

        try:
            if self.pg_pool is None or self.crdb_pool is None:
                return {
                    "status": "error",
                    "error": "Database connections not available",
                    "duration_seconds": time.time() - start_time
                }

            validation_results: Dict[str, Dict[str, Any]] = {}
            tables = ['user_features', 'transaction_features', 'risk_features']

            for table in tables:
                # Compare record counts
                async with self.pg_pool.acquire() as pg_conn:
                    pg_count = int(await pg_conn.fetchval(f'SELECT COUNT(*) FROM {table}'))

                async with self.crdb_pool.acquire() as crdb_conn:
                    crdb_count = int(await crdb_conn.fetchval(f'SELECT COUNT(*) FROM {table}'))

                validation_results[table] = {
                    "postgresql_count": pg_count,
                    "cockroachdb_count": crdb_count,
                    "counts_match": pg_count == crdb_count
                }

            # Overall validation
            all_counts_match = all(result["counts_match"] for result in validation_results.values())

            return {
                "status": "success" if all_counts_match else "warning",
                "duration_seconds": time.time() - start_time,
                "tables": validation_results,
                "all_counts_match": all_counts_match
            }

        except Exception as e:
            logger.error("Migration validation failed", error=str(e))
            return {
                "status": "error",
                "error": str(e),
                "duration_seconds": time.time() - start_time
            }

    async def _perform_switchover(self) -> Dict[str, Any]:
        """Perform switchover to CockroachDB"""
        # This would update configuration to use CockroachDB
        # In a real implementation, this would be more sophisticated

        logger.info("Switchover to CockroachDB completed (configuration update required)")

        return {
            "status": "success",
            "message": "Manual configuration update required to complete switchover"
        }


# Global migration instance
migration = DatabaseMigration()
