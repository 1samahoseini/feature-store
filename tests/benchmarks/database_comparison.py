"""
Database performance comparison: PostgreSQL vs CockroachDB
"""

import asyncio
import time
import statistics
from datetime import datetime
from typing import Optional, List, Dict, Any

import asyncpg  # type: ignore

from src.feature_store.models import UserFeatures


class DatabaseBenchmark:
    """Benchmark PostgreSQL vs CockroachDB performance"""

    def __init__(self, pg_url: str, crdb_url: str):
        self.pg_url: str = pg_url
        self.crdb_url: str = crdb_url
        self.pg_pool: Optional[asyncpg.pool.Pool] = None
        self.crdb_pool: Optional[asyncpg.pool.Pool] = None

    async def init_connections(self) -> None:
        """Initialize database connections"""
        self.pg_pool = await asyncpg.create_pool(
            self.pg_url, min_size=5, max_size=20, command_timeout=60
        )
        self.crdb_pool = await asyncpg.create_pool(
            self.crdb_url, min_size=5, max_size=20, command_timeout=60
        )

    async def close_connections(self) -> None:
        """Close database connections"""
        if self.pg_pool:
            await self.pg_pool.close()
        if self.crdb_pool:
            await self.crdb_pool.close()

    async def benchmark_single_user_queries(self, iterations: int = 1000) -> Dict[str, Any]:
        """Benchmark single user queries"""
        test_user_ids: List[str] = [f"bench_user_{i:05d}" for i in range(100)]
        pg_times: List[float] = []
        crdb_times: List[float] = []

        async def run_single(pool: Optional[asyncpg.pool.Pool], times_list: List[float]) -> None:
            if pool is None:
                return
            async with pool.acquire() as conn:
                for i in range(iterations):
                    user_id: str = test_user_ids[i % len(test_user_ids)]
                    start_time: float = time.time()
                    await conn.fetchrow(
                        "SELECT * FROM user_features WHERE user_id = $1", user_id
                    )
                    times_list.append((time.time() - start_time) * 1000)

        await run_single(self.pg_pool, pg_times)
        await run_single(self.crdb_pool, crdb_times)

        return {
            "query_type": "single_user_lookup",
            "iterations": iterations,
            "postgresql": self._analyze_latencies(pg_times),
            "cockroachdb": self._analyze_latencies(crdb_times),
            "performance_ratio": {
                "mean": statistics.mean(crdb_times) / statistics.mean(pg_times) if pg_times and crdb_times else 0,
                "p95": self._p95(crdb_times) / self._p95(pg_times)
            }
        }

    async def benchmark_batch_queries(self, batch_sizes: List[int] = [10, 50, 100]) -> Dict[str, Any]:
        """Benchmark batch queries"""
        results: Dict[str, Any] = {}
        if not self.pg_pool or not self.crdb_pool:
            return results

        async with self.pg_pool.acquire() as pg_conn, self.crdb_pool.acquire() as crdb_conn:
            for batch_size in batch_sizes:
                test_user_ids: List[str] = [f"batch_bench_user_{i:05d}" for i in range(batch_size)]

                pg_start: float = time.time()
                pg_rows = await pg_conn.fetch(
                    "SELECT * FROM user_features WHERE user_id = ANY($1)", test_user_ids
                )
                pg_duration: float = time.time() - pg_start

                crdb_start: float = time.time()
                crdb_rows = await crdb_conn.fetch(
                    "SELECT * FROM user_features WHERE user_id = ANY($1)", test_user_ids
                )
                crdb_duration: float = time.time() - crdb_start

                results[f"batch_{batch_size}"] = {
                    "batch_size": batch_size,
                    "postgresql": {
                        "duration_seconds": pg_duration,
                        "records_returned": len(pg_rows),
                        "latency_per_record_ms": (pg_duration * 1000) / batch_size
                    },
                    "cockroachdb": {
                        "duration_seconds": crdb_duration,
                        "records_returned": len(crdb_rows),
                        "latency_per_record_ms": (crdb_duration * 1000) / batch_size
                    },
                    "performance_ratio": crdb_duration / pg_duration if pg_duration else 0
                }

        return results

    async def benchmark_write_performance(self, iterations: int = 1000) -> Dict[str, Any]:
        """Benchmark insert/update operations"""
        if not self.pg_pool or not self.crdb_pool:
            return {}

        test_features: List[UserFeatures] = [
            UserFeatures(
                user_id=f"write_bench_user_{i:05d}",
                age=25 + (i % 50),
                location_country="AE",
                location_city="Dubai",
                total_orders=i % 100,
                days_since_first_order=i % 365,
                avg_order_value=100.0 + (i % 500),
                account_verified=True,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            ) for i in range(iterations)
        ]

        async def run_write(pool: Optional[asyncpg.pool.Pool], features: List[UserFeatures]) -> List[float]:
            times: List[float] = []
            if pool is None:
                return times
            async with pool.acquire() as conn:
                for f in features:
                    start_time: float = time.time()
                    await conn.execute("""INSERT INTO user_features (
                        user_id, age, location_country, total_orders,
                        avg_order_value, account_verified, created_at, updated_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    ON CONFLICT (user_id) DO UPDATE SET
                        age = EXCLUDED.age,
                        total_orders = EXCLUDED.total_orders,
                        updated_at = EXCLUDED.updated_at
                    """, f.user_id, f.age, f.location_country,
                         f.total_orders, f.avg_order_value,
                         f.account_verified, f.created_at, f.updated_at)
                    times.append((time.time() - start_time) * 1000)
            return times

        pg_times: List[float] = await run_write(self.pg_pool, test_features)
        crdb_times: List[float] = await run_write(self.crdb_pool, test_features)

        return {
            "operation_type": "upsert_operations",
            "iterations": iterations,
            "postgresql": self._analyze_latencies(pg_times, write=True),
            "cockroachdb": self._analyze_latencies(crdb_times, write=True)
        }

    async def benchmark_analytical_queries(self) -> Dict[str, Any]:
        """Benchmark complex analytical queries"""
        if not self.pg_pool or not self.crdb_pool:
            return {}

        query = """
        SELECT uf.user_id, uf.age, uf.total_orders,
               tf.total_transactions_30d, tf.total_amount_30d,
               rf.risk_score
        FROM user_features uf
        LEFT JOIN transaction_features tf ON uf.user_id = tf.user_id
        LEFT JOIN risk_features rf ON uf.user_id = rf.user_id
        WHERE uf.total_orders > $1
        ORDER BY uf.avg_order_value DESC
        LIMIT 100
        """
        iterations: int = 10
        pg_times: List[float] = []
        crdb_times: List[float] = []
        min_orders: int = 5

        async def run_join(pool: Optional[asyncpg.pool.Pool], times_list: List[float]) -> None:
            if pool is None:
                return
            async with pool.acquire() as conn:
                for _ in range(iterations):
                    start_time = time.time()
                    await conn.fetch(query, min_orders)
                    times_list.append((time.time() - start_time) * 1000)

        await run_join(self.pg_pool, pg_times)
        await run_join(self.crdb_pool, crdb_times)

        return {
            "query_type": "complex_join",
            "iterations": iterations,
            "postgresql": self._analyze_latencies(pg_times),
            "cockroachdb": self._analyze_latencies(crdb_times),
            "performance_ratio": statistics.mean(crdb_times)/statistics.mean(pg_times) if pg_times and crdb_times else 0
        }

    # ----------------- Utility functions -----------------
    def _analyze_latencies(self, times: List[float], write: bool = False) -> Dict[str, Any]:
        return {
            "mean_latency_ms": statistics.mean(times) if times else 0,
            "p50_latency_ms": statistics.median(times) if times else 0,
            "p95_latency_ms": self._p95(times),
            **({"writes_per_second": 1000/statistics.mean(times)} if write and times else {})
        }

    def _p95(self, times: List[float]) -> float:
        if len(times) > 20:
            return statistics.quantiles(times, n=20)[-2]
        return 0.0

    # ----------------- NEW: Combined Benchmark -----------------
    async def run_benchmark(self) -> Dict[str, Any]:
        """Run all benchmarks and return results in a single dictionary"""
        results: Dict[str, Any] = {}

        results["single_user"] = await self.benchmark_single_user_queries(100)
        results["batch_queries"] = await self.benchmark_batch_queries([10, 50, 100])
        results["write_performance"] = await self.benchmark_write_performance(100)
        results["analytical_queries"] = await self.benchmark_analytical_queries()

        return results


# ----------------- Run Benchmark -----------------
async def run_database_benchmark() -> None:
    pg_url = "postgresql://postgres:password@localhost:5432/feature_store"
    crdb_url = "postgresql://root@localhost:26257/feature_store?sslmode=disable"

    benchmark = DatabaseBenchmark(pg_url, crdb_url)
    await benchmark.init_connections()
    try:
        # Run the combined benchmark instead of calling each individually
        results = await benchmark.run_benchmark()
        print(results)
    finally:
        await benchmark.close_connections()


if __name__ == "__main__":
    asyncio.run(run_database_benchmark())
