"""
Performance benchmark comparing REST vs gRPC protocols
"""

import asyncio
import time
import statistics
from httpx import AsyncClient

from src.main import app
# Import FeatureStoreServicer unchanged
from src.feature_store.grpc_service import FeatureStoreServicer

# Import protobuf message classes directly (preferred).
# Provide a fallback in case generated module layout differs.
try:
    # Preferred direct imports (helps linters/mypy find names)
    from src.proto.feature_store_pb2 import UserFeatureRequest, BatchFeatureRequest # type: ignore
except Exception:
    # Fallback: import module and reference attributes (keeps old behaviour)
    from src.proto import feature_store_pb2 as _feature_store_pb2  # type: ignore
    UserFeatureRequest = _feature_store_pb2.UserFeatureRequest  # type: ignore
    BatchFeatureRequest = _feature_store_pb2.BatchFeatureRequest  # type: ignore


class RESTvsGRPCBenchmark:
    """Benchmark REST vs gRPC performance"""

    def __init__(self):
        self.test_users = [f"benchmark_user_{i:03d}" for i in range(100)]
        self.servicer = FeatureStoreServicer()

    async def benchmark_rest_api(self, iterations: int = 1000) -> dict:
        """Benchmark REST API performance"""
        response_times = []
        errors = 0

        async with AsyncClient(app=app, base_url="http://test", timeout=30.0) as client:
            # Warm up
            for user_id in self.test_users[:10]:
                await client.get(f"/api/v1/features/user/{user_id}")

            # Benchmark
            start_time = time.time()

            for i in range(iterations):
                user_id = self.test_users[i % len(self.test_users)]

                try:
                    request_start = time.time()
                    response = await client.get(f"/api/v1/features/user/{user_id}")
                    request_end = time.time()

                    if response.status_code == 200:
                        latency_ms = (request_end - request_start) * 1000
                        response_times.append(latency_ms)

                        # Also get application-reported time (if present)
                        data = response.json()
                        data.get("response_time_ms", 0)

                    else:
                        errors += 1

                except Exception:
                    errors += 1

            total_duration = time.time() - start_time

        return {
            "protocol": "REST",
            "iterations": iterations,
            "total_duration_seconds": total_duration,
            "throughput_rps": iterations / total_duration if total_duration > 0 else 0.0,
            "error_count": errors,
            "error_rate": errors / iterations if iterations > 0 else 1.0,
            "response_times_ms": response_times,
            "mean_latency_ms": statistics.mean(response_times) if response_times else 0,
            "p50_latency_ms": statistics.median(response_times) if response_times else 0,
            "p95_latency_ms": statistics.quantiles(response_times, n=20)[18] if len(response_times) > 20 else 0,
            "p99_latency_ms": statistics.quantiles(response_times, n=100)[98] if len(response_times) > 100 else 0
        }

    async def benchmark_grpc_api(self, iterations: int = 1000) -> dict:
        """Benchmark gRPC API performance"""
        response_times = []
        errors = 0

        # Warm up cache via REST first
        async with AsyncClient(app=app, base_url="http://test") as client:
            for user_id in self.test_users[:10]:
                await client.get(f"/api/v1/features/user/{user_id}")

        # Benchmark gRPC
        start_time = time.time()

        for i in range(iterations):
            user_id = self.test_users[i % len(self.test_users)]

            try:
                request = UserFeatureRequest(
                    user_id=user_id,
                    feature_types=["user"]
                )

                context = MockGRPCContext()

                request_start = time.time()
                response = await self.servicer.GetUserFeatures(request, context)
                request_end = time.time()

                # The servicer is expected to return a response with user_id
                if getattr(response, "user_id", None) == user_id:
                    latency_ms = (request_end - request_start) * 1000
                    response_times.append(latency_ms)
                else:
                    errors += 1

            except Exception:
                errors += 1

        total_duration = time.time() - start_time

        return {
            "protocol": "gRPC",
            "iterations": iterations,
            "total_duration_seconds": total_duration,
            "throughput_rps": iterations / total_duration if total_duration > 0 else 0.0,
            "error_count": errors,
            "error_rate": errors / iterations if iterations > 0 else 1.0,
            "response_times_ms": response_times,
            "mean_latency_ms": statistics.mean(response_times) if response_times else 0,
            "p50_latency_ms": statistics.median(response_times) if response_times else 0,
            "p95_latency_ms": statistics.quantiles(response_times, n=20)[18] if len(response_times) > 20 else 0,
            "p99_latency_ms": statistics.quantiles(response_times, n=100)[98] if len(response_times) > 100 else 0
        }

    async def benchmark_batch_requests(self, batch_sizes: list = [10, 50, 100]) -> dict:
        """Benchmark batch request performance for different sizes"""
        results = {}

        for batch_size in batch_sizes:
            # REST batch benchmark
            batch_users = self.test_users[:batch_size]
            batch_request = {
                "requests": [
                    {"user_id": user_id, "feature_types": ["user"]}
                    for user_id in batch_users
                ]
            }

            async with AsyncClient(app=app, base_url="http://test", timeout=60.0) as client:
                # Measure REST batch
                rest_start = time.time()
                rest_response = await client.post("/api/v1/features/batch", json=batch_request)
                rest_duration = time.time() - rest_start

                rest_success = rest_response.status_code == 200
                rest_rps = batch_size / rest_duration if rest_success and rest_duration > 0 else 0

            # gRPC batch benchmark
            grpc_requests = []
            for user_id in batch_users:
                grpc_req = UserFeatureRequest(
                    user_id=user_id,
                    feature_types=["user"]
                )
                grpc_requests.append(grpc_req)

            grpc_batch_request = BatchFeatureRequest(
                requests=grpc_requests
            )

            context = MockGRPCContext()

            grpc_start = time.time()
            grpc_response = await self.servicer.GetBatchFeatures(grpc_batch_request, context)
            grpc_duration = time.time() - grpc_start

            grpc_success = getattr(grpc_response, "total_requests", 0) == batch_size
            grpc_rps = batch_size / grpc_duration if grpc_success and grpc_duration > 0 else 0

            results[f"batch_size_{batch_size}"] = {
                "batch_size": batch_size,
                "rest": {
                    "duration_seconds": rest_duration,
                    "throughput_rps": rest_rps,
                    "success": rest_success,
                    "latency_per_user_ms": (rest_duration * 1000) / batch_size if rest_success and batch_size > 0 else 0
                },
                "grpc": {
                    "duration_seconds": grpc_duration,
                    "throughput_rps": grpc_rps,
                    "success": grpc_success,
                    "latency_per_user_ms": (grpc_duration * 1000) / batch_size if grpc_success and batch_size > 0 else 0
                },
                "grpc_advantage": grpc_rps / rest_rps if rest_rps > 0 else 0
            }

        return results

    async def run_comprehensive_benchmark(self) -> dict:
        """Run comprehensive performance benchmark"""
        print("Starting comprehensive REST vs gRPC benchmark...")

        # Single request benchmarks
        print("Benchmarking single requests...")
        rest_results = await self.benchmark_rest_api(1000)
        grpc_results = await self.benchmark_grpc_api(1000)

        # Batch request benchmarks
        print("Benchmarking batch requests...")
        batch_results = await self.benchmark_batch_requests([10, 50, 100])

        # Compile comprehensive results
        comprehensive_results = {
            "benchmark_timestamp": time.time(),
            "single_request_comparison": {
                "rest": rest_results,
                "grpc": grpc_results,
                "grpc_latency_advantage": {
                    "mean": (rest_results["mean_latency_ms"] - grpc_results["mean_latency_ms"]) / rest_results["mean_latency_ms"] * 100 if rest_results["mean_latency_ms"] > 0 else 0,
                    "p95": (rest_results["p95_latency_ms"] - grpc_results["p95_latency_ms"]) / rest_results["p95_latency_ms"] * 100 if grpc_results["p95_latency_ms"] > 0 else 0
                },
                "grpc_throughput_advantage": (grpc_results["throughput_rps"] / rest_results["throughput_rps"] - 1) if rest_results["throughput_rps"] > 0 else 0
            },
            "batch_request_comparison": batch_results,
            "recommendations": self._generate_performance_recommendations(rest_results, grpc_results, batch_results)
        }

        return comprehensive_results

    def _generate_performance_recommendations(self, rest_results: dict, grpc_results: dict, batch_results: dict) -> list:
        """Generate performance optimization recommendations"""
        recommendations = []

        # Latency recommendations
        if rest_results["p95_latency_ms"] > 40:
            recommendations.append("REST P95 latency exceeds 40ms SLA - consider optimization")

        if grpc_results["p95_latency_ms"] > 30:
            recommendations.append("gRPC P95 latency exceeds 30ms SLA - consider optimization")

        # Throughput recommendations
        if rest_results["throughput_rps"] < 1000:
            recommendations.append("REST throughput below 1000 RPS target")

        if grpc_results["throughput_rps"] < 2000:
            recommendations.append("gRPC throughput below 2000 RPS target")

        # Protocol selection recommendations
        grpc_advantage = (grpc_results["throughput_rps"] / rest_results["throughput_rps"]) if rest_results["throughput_rps"] > 0 else 0
        if grpc_advantage > 1.5:
            recommendations.append("gRPC shows significant performance advantage - prioritize for high-volume clients")
        elif grpc_advantage < 1.2:
            recommendations.append("gRPC advantage minimal - REST may be sufficient for most use cases")

        # Batch size recommendations
        for batch_size_key, results in batch_results.items():
            if results["grpc"]["latency_per_user_ms"] > 20:
                recommendations.append(f"Batch size {results['batch_size']} shows high per-user latency - consider smaller batches")

        return recommendations


class MockGRPCContext:
    """Mock gRPC context for testing"""

    def __init__(self):
        self.code = None
        self.details = None

    def set_code(self, code):
        self.code = code

    def set_details(self, details):
        self.details = details


async def run_benchmark():
    """Run the benchmark and print results"""
    benchmark = RESTvsGRPCBenchmark()
    results = await benchmark.run_comprehensive_benchmark()

    print("\n" + "="*50)
    print("FEATURE STORE PERFORMANCE BENCHMARK RESULTS")
    print("="*50)

    # Single request results
    rest = results["single_request_comparison"]["rest"]
    grpc = results["single_request_comparison"]["grpc"]

    print("\nSINGLE REQUEST PERFORMANCE:")
    print(f"REST - Mean: {rest['mean_latency_ms']:.2f}ms, P95: {rest['p95_latency_ms']:.2f}ms, RPS: {rest['throughput_rps']:.0f}")
    print(f"gRPC - Mean: {grpc['mean_latency_ms']:.2f}ms, P95: {grpc['p95_latency_ms']:.2f}ms, RPS: {grpc['throughput_rps']:.0f}")

    advantages = results["single_request_comparison"]["grpc_latency_advantage"]
    print(f"gRPC Latency Advantage - Mean: {advantages['mean']:.1f}%, P95: {advantages['p95']:.1f}%")

    # Batch results
    print("\nBATCH REQUEST PERFORMANCE:")
    for batch_name, batch_data in results["batch_request_comparison"].items():
        size = batch_data["batch_size"]
        rest_per_user = batch_data["rest"]["latency_per_user_ms"]
        grpc_per_user = batch_data["grpc"]["latency_per_user_ms"]
        print(f"Batch {size} - REST: {rest_per_user:.2f}ms/user, gRPC: {grpc_per_user:.2f}ms/user")

    # Recommendations
    print("\nRECOMMENDATIONS:")
    for rec in results["recommendations"]:
        print(f"• {rec}")

    return results


if __name__ == "__main__":
    asyncio.run(run_benchmark())
