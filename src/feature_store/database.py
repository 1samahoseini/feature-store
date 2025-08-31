# -*- coding: utf-8 -*-
"""
Database Layer
Async connection pool with PostgreSQL (Cloud SQL)
"""

from typing import Dict, Any, List, Optional
import asyncpg
import structlog

from src.config.settings import get_settings

logger = structlog.get_logger(__name__)


class Database:
    """PostgreSQL database layer using asyncpg connection pool"""

    def __init__(self):
        self.settings = get_settings()
        self.new_method()

    def new_method(self) -> None:
        # Pool is initialized lazily in init()
        self.pool: Optional[asyncpg.Pool] = None

    async def init(self) -> None:
        """Initialize database connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                dsn=self.settings.database_url,
                min_size=1,
                max_size=self.settings.database_pool_size,
                max_inactive_connection_lifetime=300,
                command_timeout=60,
            )
            logger.info("Database connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    async def close(self) -> None:
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")

    async def fetch_user_features(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Fetch user-level features"""
        if not self.pool:
            return None

        query = """
            SELECT *
            FROM user_features
            WHERE user_id = $1
        """

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, user_id)
            return dict(row) if row else None

    async def fetch_transaction_features(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """Fetch transaction-level features"""
        if not self.pool:
            return None

        query = """
            SELECT *
            FROM transaction_features
            WHERE transaction_id = $1
        """

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, transaction_id)
            return dict(row) if row else None

    async def fetch_risk_features(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Fetch risk-related features"""
        if not self.pool:
            return None

        query = """
            SELECT *
            FROM risk_features
            WHERE user_id = $1
        """

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, user_id)
            return dict(row) if row else None

    async def _get_batch_features_chunk(
        self, user_ids: List[str], feature_types: List[str]
    ) -> List[Dict[str, Any]]:
        """Fetch batch features for a chunk of user IDs"""
        if not self.pool:
            return []

        results: List[Dict[str, Any]] = []

        async with self.pool.acquire() as conn:
            if "user" in feature_types:
                query = "SELECT * FROM user_features WHERE user_id = ANY($1)"
                rows = await conn.fetch(query, user_ids)
                results.extend([dict(row) for row in rows])

            if "transaction" in feature_types:
                query = "SELECT * FROM transaction_features WHERE user_id = ANY($1)"
                rows = await conn.fetch(query, user_ids)
                results.extend([dict(row) for row in rows])

            if "risk" in feature_types:
                query = "SELECT * FROM risk_features WHERE user_id = ANY($1)"
                rows = await conn.fetch(query, user_ids)
                results.extend([dict(row) for row in rows])

        return results

    async def get_batch_features(
        self, user_ids: List[str], feature_types: List[str], batch_size: int = 100
    ) -> List[Dict[str, Any]]:
        """Fetch batch features for multiple users"""
        if not self.pool:
            return []

        chunks = [user_ids[i:i + batch_size] for i in range(0, len(user_ids), batch_size)]

        results: List[Dict[str, Any]] = []
        for chunk in chunks:
            rows = await self._get_batch_features_chunk(chunk, feature_types)
            results.extend(rows)

        return results

    async def health_check(self) -> bool:
        """Check database health"""
        if not self.pool:
            return False

        try:
            async with self.pool.acquire() as conn:
                await conn.execute("SELECT 1")
            return True
        except Exception:
            return False


# -----------------------------------------------------------------------------
# Backwards-compat exports (used by other modules/tests)
# -----------------------------------------------------------------------------

# Some modules import `db` directly
database = Database()
db = database

# Some scripts expect a function named `init_db`
async def init_db() -> None:
    """Backwards-compat initializer used by some scripts."""
    await database.init()

# Some tests import `DatabaseManager`
class DatabaseManager(Database):
    """Backwards-compat alias of Database."""
    pass

__all__ = [
    "Database",
    "DatabaseManager",
    "database",
    "db",
    "init_db",
]
