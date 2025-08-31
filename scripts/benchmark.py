"""
Performance benchmarking script for feature store
"""

import asyncio
import argparse
import json
import time
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

import structlog
from httpx import AsyncClient

from tests.benchmarks.rest_vs_grpc import RESTvsGRPCBenchmark  # type: ignore
from tests.benchmarks.database_comparison import DatabaseBenchmark  # type: ignore

logger = structlog.get_logger(__name__)


class FeatureStoreBenchmark:
    """Comprehensive feature store benchmarking"""

    def __init__(self, api_base_url: str = "http://localhost:8000") -> None:
        self.api_base_url: str = api_base_url
        self.results: Dict[str, Any] = {}

    async def run_api_benchmarks(self) -> Dict[str, Any]:
        """Run API performance benchmarks"""
        logger.info("Running API benchmarks...")
        protocol_benchmark: RESTvsGRPCBenchmark = RESTvsGRPCBenchmark()
        protocol_results: Dict[str, Any] = await protocol_benchmark.run_comprehensive_benchmark()
        self.results['protocol_comparison'] = protocol_results
        return protocol_results

    async def run_database_benchmarks(self, pg_url: str, crdb_url: str) -> Dict[str, Any]:
        """Run database performance benchmarks"""
        logger.info("Running database benchmarks...")
        db_benchmark: DatabaseBenchmark = DatabaseBenchmark(pg_url, crdb_url)
        db_results: Dict[str, Any] = await self.new_method(db_benchmark)()
        self.results['database_comparison'] = db_results
        return db_results

    def new_method(self, db_benchmark: DatabaseBenchmark):
        """Helper to return the benchmark method"""
        return db_benchmark.run_benchmark

    async def run_load_test(self, duration_minutes: int = 5, target_rps: int = 100) -> Dict[str, Any]:
        """Run sustained load test"""
        logger.info(f"Running {duration_minutes}-minute load test at {target_rps} RPS...")
        duration_seconds: int = duration_minutes * 60
        test_users: List[str] = [f"load_test_user_{i:03d}" for i in range(100)]

        stats: Dict[str, Any] = {
            'start_time': datetime.utcnow().isoformat(),
            'duration_seconds': duration_seconds,
            'target_rps': target_rps,
            'response_times': [],
            'errors': [],
            'status_codes': {}
        }

        async def load_worker() -> int:
            async with AsyncClient(base_url=self.api_base_url, timeout=30.0) as client:
                worker_start: float = time.time()
                requests_made: int = 0
                while time.time() - worker_start < duration_seconds:
                    try:
                        user_id: str = test_users[requests_made % len(test_users)]
                        start: float = time.time()
                        response = await client.get(f"/api/v1/features/user/{user_id}")
                        end: float = time.time()

                        stats['response_times'].append((end - start) * 1000)
                        status: int = response.status_code
                        stats['status_codes'][status] = stats['status_codes'].get(status, 0) + 1
                        if status >= 400:
                            stats['errors'].append(f"HTTP {status}")

                        requests_made += 1
                        await asyncio.sleep(1.0 / target_rps)
                    except Exception as e:
                        stats['errors'].append(str(e))
                return requests_made

        start_time: float = time.time()
        worker_results: List[int] = await asyncio.gather(*[load_worker() for _ in range(5)])
        actual_duration: float = time.time() - start_time
        total_requests: int = sum(worker_results)
        actual_rps: float = total_requests / actual_duration if actual_duration > 0 else 0.0

        response_times: List[float] = stats['response_times']
        stats.update({
            'end_time': datetime.utcnow().isoformat(),
            'actual_duration_seconds': actual_duration,
            'total_requests': total_requests,
            'actual_rps': actual_rps,
            'error_count': len(stats['errors']),
            'error_rate': len(stats['errors']) / total_requests if total_requests > 0 else 1.0,
            'avg_response_time_ms': sum(response_times) / len(response_times) if response_times else 0.0,
            'p95_response_time_ms': sorted(response_times)[int(0.95 * len(response_times))] if len(response_times) > 20 else 0.0
        })

        self.results['load_test'] = stats
        logger.info(f"Load test completed: {actual_rps:.1f} RPS, {stats['error_rate']:.2%} error rate")
        return stats

    async def run_scalability_test(self, max_concurrent_users: int = 100) -> Dict[str, Any]:
        """Test scalability with increasing concurrent users"""
        logger.info(f"Running scalability test up to {max_concurrent_users} concurrent users...")
        test_results: List[Dict[str, Any]] = []

        concurrency_levels: List[int] = [1, 5, 10, 20, 50, 100]
        if max_concurrent_users not in concurrency_levels:
            concurrency_levels.append(max_concurrent_users)
        concurrency_levels = sorted([c for c in concurrency_levels if c <= max_concurrent_users])

        for concurrency in concurrency_levels:
            logger.info(f"Testing {concurrency} concurrent users...")
            test_users: List[str] = [f"scale_test_user_{i:03d}" for i in range(concurrency)]

            async def concurrent_worker(user_id: str) -> Tuple[List[float], int]:
                async with AsyncClient(base_url=self.api_base_url, timeout=30.0) as client:
                    response_times: List[float] = []
                    errors: int = 0
                    for _ in range(10):
                        try:
                            start: float = time.time()
                            response = await client.get(f"/api/v1/features/user/{user_id}")
                            end: float = time.time()
                            if response.status_code == 200:
                                response_times.append((end - start) * 1000)
                            else:
                                errors += 1
                        except Exception:
                            errors += 1
                    return response_times, errors

            start_time: float = time.time()
            worker_results: List[Tuple[List[float], int]] = await asyncio.gather(
                *[concurrent_worker(user_id) for user_id in test_users]
            )
            duration: float = time.time() - start_time

            all_response_times: List[float] = []
            total_errors: int = 0
            for response_times, errors in worker_results:
                all_response_times.extend(response_times)
                total_errors += errors

            total_requests: int = concurrency * 10
            throughput: float = total_requests / duration if duration > 0 else 0.0

            test_results.append({
                'concurrent_users': concurrency,
                'total_requests': total_requests,
                'duration_seconds': duration,
                'throughput_rps': throughput,
                'error_count': total_errors,
                'error_rate': total_errors / total_requests if total_requests > 0 else 1.0,
                'avg_response_time_ms': sum(all_response_times) / len(all_response_times) if all_response_times else 0.0,
                'p95_response_time_ms': sorted(all_response_times)[int(0.95 * len(all_response_times))] if len(all_response_times) > 20 else 0.0
            })

        scalability_results: Dict[str, Any] = {
            'test_type': 'scalability',
            'max_concurrent_users': max_concurrent_users,
            'results': test_results,
            'analysis': self._analyze_scalability_results(test_results)
        }

        self.results['scalability_test'] = scalability_results
        return scalability_results

    def _analyze_scalability_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze scalability test results"""
        analysis: Dict[str, Any] = {
            'linear_scalability': True,
            'degradation_points': [],
            'max_sustainable_rps': 0.0,
            'recommendations': []
        }

        prev_rps: float = 0.0
        for result in results:
            current_rps: float = result['throughput_rps']
            if prev_rps > 0 and current_rps < prev_rps * 0.8:
                analysis['linear_scalability'] = False
                analysis['degradation_points'].append({
                    'concurrent_users': result['concurrent_users'],
                    'throughput_rps': current_rps,
                    'degradation': (prev_rps - current_rps) / prev_rps
                })
            if result['error_rate'] < 0.05:
                analysis['max_sustainable_rps'] = max(analysis['max_sustainable_rps'], current_rps)
            prev_rps = current_rps

        if not analysis['linear_scalability']:
            analysis['recommendations'].append(
                "Performance degrades under high concurrency - consider scaling infrastructure"
            )
        if analysis['max_sustainable_rps'] < 1000:
            analysis['recommendations'].append(
                "Maximum sustainable RPS below target - optimize bottlenecks"
            )

        return analysis

    def generate_report(self, output_file: Optional[str] = None) -> str:
        """Generate comprehensive benchmark report"""
        report: Dict[str, Any] = {
            'benchmark_timestamp': datetime.utcnow().isoformat(),
            'summary': self._generate_summary(),
            'results': self.results,
            'recommendations': self._generate_recommendations()
        }
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2)
            logger.info(f"Benchmark report saved to {output_file}")
        return json.dumps(report, indent=2)

    def _generate_summary(self) -> Dict[str, Any]:
        """Generate benchmark summary"""
        summary: Dict[str, Any] = {}
        if 'protocol_comparison' in self.results:
            protocol_data = self.results['protocol_comparison'].get('single_request_comparison', {})
            summary['api_performance'] = {
                'rest_p95_latency_ms': protocol_data.get('rest', {}).get('p95_latency_ms', 0),
                'grpc_p95_latency_ms': protocol_data.get('grpc', {}).get('p95_latency_ms', 0),
                'grpc_advantage_percent': protocol_data.get('grpc_latency_advantage', {}).get('p95', 0)
            }
        if 'load_test' in self.results:
            load_data = self.results['load_test']
            summary['load_test'] = {
                'achieved_rps': load_data.get('actual_rps', 0.0),
                'error_rate': load_data.get('error_rate', 0.0),
                'p95_latency_ms': load_data.get('p95_response_time_ms', 0.0)
            }
        if 'scalability_test' in self.results:
            scale_data = self.results['scalability_test']
            summary['scalability'] = {
                'max_sustainable_rps': scale_data['analysis'].get('max_sustainable_rps', 0.0),
                'linear_scalability': scale_data['analysis'].get('linear_scalability', True)
            }
        return summary

    def _generate_recommendations(self) -> List[str]:
        """Generate optimization recommendations"""
        recommendations: List[str] = []
        if 'protocol_comparison' in self.results:
            recommendations.extend(self.results['protocol_comparison'].get('recommendations', []))
        if 'database_comparison' in self.results:
            recommendations.extend(self.results['database_comparison'].get('recommendations', []))
        if 'load_test' in self.results:
            load_data = self.results['load_test']
            if load_data.get('error_rate', 0.0) > 0.05:
                recommendations.append("High error rate under load - investigate bottlenecks")
            if load_data.get('p95_response_time_ms', 0.0) > 100:
                recommendations.append("High P95 latency under load - optimize critical path")
        if 'scalability_test' in self.results:
            recommendations.extend(self.results['scalability_test']['analysis'].get('recommendations', []))
        return list(set(recommendations))  # deduplicate


async def main() -> None:
    parser = argparse.ArgumentParser(description='Run feature store performance benchmarks')
    parser.add_argument('--api-url', default='http://localhost:8000', help='API base URL')
    parser.add_argument('--pg-url', help='PostgreSQL connection URL')
    parser.add_argument('--crdb-url', help='CockroachDB connection URL')
    parser.add_argument('--skip-api', action='store_true', help='Skip API benchmarks')
    parser.add_argument('--skip-db', action='store_true', help='Skip database benchmarks')
    parser.add_argument('--skip-load', action='store_true', help='Skip load testing')
    parser.add_argument('--skip-scale', action='store_true', help='Skip scalability testing')
    parser.add_argument('--load-duration', type=int, default=5, help='Load test duration (minutes)')
    parser.add_argument('--load-rps', type=int, default=100, help='Load test target RPS')
    parser.add_argument('--max-users', type=int, default=100, help='Max concurrent users for scalability test')
    parser.add_argument('--output', help='Output file for benchmark report')

    args = parser.parse_args()
    benchmark = FeatureStoreBenchmark(args.api_url)

    try:
        if not args.skip_api:
            await benchmark.run_api_benchmarks()
        if not args.skip_db and args.pg_url and args.crdb_url:
            await benchmark.run_database_benchmarks(args.pg_url, args.crdb_url)
        if not args.skip_load:
            await benchmark.run_load_test(args.load_duration, args.load_rps)
        if not args.skip_scale:
            await benchmark.run_scalability_test(args.max_users)
        report: str = benchmark.generate_report(args.output)
        if not args.output:
            print(report)
        logger.info("Benchmark completed successfully")
    except Exception as e:
        logger.error(f"Benchmark failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
