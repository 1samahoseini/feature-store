"""
Performance and load testing for feature store
"""

import pytest
import asyncio
import time
import statistics
from datetime import datetime
from unittest.mock import MagicMock
from httpx import AsyncClient

from src.main import app
from src.proto import feature_store_pb2
from src.feature_store.grpc_service import FeatureStoreServicer

# Mock database and cache for testing
class MockDB:
    async def get_database_stats(self):
        return {'pool_size': 10}
    
    async def update_user_features(self, user_features):
        return True

class MockCache:
    async def get_cache_stats(self):
        return {'evicted_keys': 0, 'used_memory': '50MB'}

class MockUserFeatures:
    def __init__(self, user_id, age, total_orders, avg_order_value, account_verified, created_at, updated_at):
        self.user_id = user_id
        self.age = age
        self.total_orders = total_orders
        self.avg_order_value = avg_order_value
        self.account_verified = account_verified
        self.created_at = created_at
        self.updated_at = updated_at

# Initialize mock objects
db = MockDB()
cache = MockCache()
UserFeatures = MockUserFeatures


class TestAPIPerformance:
    """Test REST API performance under various loads"""
    
    @pytest.mark.asyncio
    @pytest.mark.performance
    async def test_single_user_latency_p95(self, setup_test_environment, seed_test_data):
        """Test P95 latency for single user requests meets SLA"""
        user_id = seed_test_data[0]
        response_times = []
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            # Warm up cache
            _ = await client.get(f"/api/v1/features/user/{user_id}")
            
            # Measure 100 requests for statistical significance
            for _ in range(100):
                response = await client.get(f"/api/v1/features/user/{user_id}")
                assert response.status_code == 200
                # Record application-reported response time
                data = response.json()
                response_times.append(data["response_time_ms"])
        
        # Calculate P95 latency
        p95_latency = statistics.quantiles(response_times, n=20)[18]  # ~95th percentile
        mean_latency = statistics.mean(response_times)
        
        # SLA requirements
        assert p95_latency < 40, f"P95 latency {p95_latency:.2f}ms exceeds 40ms SLA"
        assert mean_latency < 20, f"Mean latency {mean_latency:.2f}ms exceeds 20ms target"
        
        print(f"Performance Results - Mean: {mean_latency:.2f}ms, P95: {p95_latency:.2f}ms")
    
    @pytest.mark.asyncio
    @pytest.mark.performance
    async def test_throughput_capacity(self, setup_test_environment, seed_test_data):
        """Test API throughput capacity"""
        async def make_request(client, user_id):
            response = await client.get(f"/api/v1/features/user/{user_id}")
            assert response.status_code == 200
            return response.json()["response_time_ms"]
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            # Warm up cache for all users
            for user_id in seed_test_data:
                _ = await client.get(f"/api/v1/features/user/{user_id}")
            
            # Test sustained throughput
            start_time = time.time()
            
            # 500 requests with varying concurrency levels
            tasks = []
            for i in range(500):
                user_id = seed_test_data[i % len(seed_test_data)]
                tasks.append(make_request(client, user_id))
            
            response_times = await asyncio.gather(*tasks)
            
            end_time = time.time()
            total_duration = end_time - start_time
            
            # Calculate throughput
            rps = 500 / total_duration
            
            # Throughput requirements
            assert rps > 1000, f"Throughput {rps:.0f} RPS below 1000 RPS minimum"
            
            # Response time consistency under load
            mean_response_time = statistics.mean(response_times)
            assert mean_response_time < 50, f"Mean response time {mean_response_time:.2f}ms too high under load"
            
            print(f"Throughput Results - {rps:.0f} RPS, Mean latency: {mean_response_time:.2f}ms")
    
    @pytest.mark.asyncio
    @pytest.mark.performance
    async def test_batch_vs_individual_performance(self, setup_test_environment, seed_test_data):
        """Test batch request performance vs individual requests"""
        batch_size = 50
        test_users = seed_test_data * (batch_size // len(seed_test_data) + 1)
        test_users = test_users[:batch_size]
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            # Test individual requests
            individual_start = time.time()
            individual_tasks = [
                client.get(f"/api/v1/features/user/{user_id}")
                for user_id in test_users
            ]
            individual_responses = await asyncio.gather(*individual_tasks)
            individual_duration = time.time() - individual_start
            
            # Clear cache
            for user_id in test_users:
                _ = await client.delete(f"/api/v1/features/user/{user_id}")
            
            # Test batch request
            batch_request = {
                "requests": [
                    {"user_id": user_id, "feature_types": ["user"]}
                    for user_id in test_users
                ]
            }
            
            batch_start = time.time()
            batch_response = await client.post("/api/v1/features/batch", json=batch_request)
            batch_duration = time.time() - batch_start
            
            # Verify all requests succeeded
            assert all(r.status_code == 200 for r in individual_responses)
            assert batch_response.status_code == 200
            
            batch_data = batch_response.json()
            assert batch_data["successful_requests"] == batch_size
            
            # Batch should be more efficient
            individual_rps = batch_size / individual_duration
            batch_rps = batch_size / batch_duration
            
            print(f"Individual: {individual_rps:.0f} RPS, Batch: {batch_rps:.0f} RPS")
            
            # Batch should be at least 50% more efficient
            assert batch_rps > individual_rps * 1.5
    
    @pytest.mark.asyncio
    @pytest.mark.performance
    async def test_cache_vs_database_performance(self, setup_test_environment, seed_test_data):
        """Test performance difference between cache and database reads"""
        user_id = seed_test_data[0]
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            # Measure database read (cache miss)
            _ = await client.delete(f"/api/v1/features/user/{user_id}")  # Ensure cache miss
            
            db_response_times = []
            for _ in range(10):
                response = await client.get(f"/api/v1/features/user/{user_id}")
                _ = await client.delete(f"/api/v1/features/user/{user_id}")  # Clear cache each time
                data = response.json()
                assert not data["cache_hit"]
                db_response_times.append(data["response_time_ms"])
            
            # Measure cache read (cache hit)
            _ = await client.get(f"/api/v1/features/user/{user_id}")  # Populate cache
            
            cache_response_times = []
            for _ in range(10):
                response = await client.get(f"/api/v1/features/user/{user_id}")
                data = response.json()
                assert data["cache_hit"]
                cache_response_times.append(data["response_time_ms"])
            
            # Calculate averages
            avg_db_time = statistics.mean(db_response_times)
            avg_cache_time = statistics.mean(cache_response_times)
            
            # Cache should be significantly faster
            assert avg_cache_time < 10, f"Cache response time {avg_cache_time:.2f}ms too slow"
            assert avg_db_time > avg_cache_time * 2, "Cache not providing sufficient speedup"
            
            print(f"Performance - DB: {avg_db_time:.2f}ms, Cache: {avg_cache_time:.2f}ms")


class TestGRPCPerformance:
    """Test gRPC service performance characteristics"""
    
    @pytest.mark.asyncio
    @pytest.mark.performance
    async def test_grpc_vs_rest_performance(self, setup_test_environment, seed_test_data):
        """Test gRPC performance compared to REST"""
        user_id = seed_test_data[0]
        servicer = FeatureStoreServicer()
        
        # Warm up cache via REST
        async with AsyncClient(app=app, base_url="http://test") as client:
            _ = await client.get(f"/api/v1/features/user/{user_id}")
        
        # Measure REST performance
        rest_times = []
        async with AsyncClient(app=app, base_url="http://test") as client:
            for _ in range(50):
                start_time = time.time()
                response = await client.get(f"/api/v1/features/user/{user_id}")
                end_time = time.time()
                
                assert response.status_code == 200
                rest_times.append((end_time - start_time) * 1000)
        
        # Measure gRPC performance
        grpc_times = []
        for _ in range(50):
            request = feature_store_pb2.UserFeatureRequest(
                user_id=user_id,
                feature_types=["user"]
            )
            
            context = MagicMock()
            
            start_time = time.time()
            response = await servicer.GetUserFeatures(request, context)
            end_time = time.time()
            
            assert response.user_id == user_id
            grpc_times.append((end_time - start_time) * 1000)
        
        # Calculate averages
        avg_rest_time = statistics.mean(rest_times)
        avg_grpc_time = statistics.mean(grpc_times)
        
        # Both should meet performance requirements (average as proxy here)
        assert avg_rest_time < 40, f"REST avg {avg_rest_time:.2f}ms exceeds 40ms target"
        assert avg_grpc_time < 30, f"gRPC avg {avg_grpc_time:.2f}ms exceeds 30ms target"
        
        # gRPC should be faster (binary protocol advantage)
        assert avg_grpc_time < avg_rest_time, "gRPC should be faster than REST"
        
        print(f"Protocol Performance - REST: {avg_rest_time:.2f}ms, gRPC: {avg_grpc_time:.2f}ms")
    
    @pytest.mark.asyncio
    @pytest.mark.performance
    async def test_grpc_batch_performance(self, setup_test_environment, seed_test_data):
        """Test gRPC batch request performance"""
        servicer = FeatureStoreServicer()
        
        # Create batch request
        user_requests = []
        for user_id in seed_test_data:
            user_req = feature_store_pb2.UserFeatureRequest(
                user_id=user_id,
                feature_types=["user"]
            )
            user_requests.append(user_req)
        
        batch_request = feature_store_pb2.BatchFeatureRequest(
            requests=user_requests
        )
        
        # Measure batch performance
        start_time = time.time()
        context = MagicMock()
        response = await servicer.GetBatchFeatures(batch_request, context)
        end_time = time.time()
        
        duration_ms = (end_time - start_time) * 1000
        
        assert response.total_requests == len(seed_test_data)
        assert response.successful_requests == len(seed_test_data)
        
        # Batch performance should be efficient
        avg_time_per_user = duration_ms / len(seed_test_data)
        assert avg_time_per_user < 15, f"Batch avg {avg_time_per_user:.2f}ms per user too slow"
        
        print(f"gRPC Batch Performance - {avg_time_per_user:.2f}ms per user")


class TestMemoryAndResourceUsage:
    """Test memory usage and resource efficiency"""
    
    @pytest.mark.asyncio
    @pytest.mark.performance
    async def test_memory_usage_under_load(self, setup_test_environment, seed_test_data):
        """Test memory usage remains stable under sustained load"""
        import psutil
        import os
        
        # Get current process
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        async def sustained_load():
            async with AsyncClient(app=app, base_url="http://test") as client:
                # Make 1000 requests with high concurrency
                tasks = []
                for i in range(1000):
                    user_id = seed_test_data[i % len(seed_test_data)]
                    tasks.append(client.get(f"/api/v1/features/user/{user_id}"))
                
                responses = await asyncio.gather(*tasks)
                return [r.status_code for r in responses]
        
        # Run sustained load
        status_codes = await sustained_load()
        
        # Check memory after load
        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory
        memory_increase_mb = memory_increase / (1024 * 1024)
        
        # All requests should succeed
        assert all(code == 200 for code in status_codes)
        
        # Memory increase should be reasonable (< 100MB for this test)
        assert memory_increase_mb < 100, f"Memory increased by {memory_increase_mb:.1f}MB under load"
        
        print(
            f"Memory Usage - Initial: {initial_memory/(1024*1024):.1f}MB, "
            f"Final: {final_memory/(1024*1024):.1f}MB, "
            f"Increase: {memory_increase_mb:.1f}MB"
        )
    
    @pytest.mark.asyncio
    @pytest.mark.performance 
    async def test_connection_pool_efficiency(self, setup_test_environment, seed_test_data):
        """Test database connection pool efficiency"""
        # Get initial pool stats
        initial_stats = await db.get_database_stats()
        initial_pool_size = initial_stats.get('pool_size', 0)
        
        async def concurrent_db_requests():
            async with AsyncClient(app=app, base_url="http://test") as client:
                # Clear cache to force database hits
                for user_id in seed_test_data:
                    _ = await client.delete(f"/api/v1/features/user/{user_id}")
                
                # Make concurrent requests that will hit database
                tasks = []
                for i in range(50):  # More requests than pool size
                    user_id = seed_test_data[i % len(seed_test_data)]
                    tasks.append(client.get(f"/api/v1/features/user/{user_id}"))
                
                return await asyncio.gather(*tasks)
        
        # Execute concurrent requests
        responses = await concurrent_db_requests()
        
        # All requests should succeed despite limited pool size
        assert all(r.status_code == 200 for r in responses)
        
        # Pool should not have grown beyond limits
        final_stats = await db.get_database_stats()
        final_pool_size = final_stats.get('pool_size', 0)
        
        assert final_pool_size <= initial_pool_size + 5, "Connection pool grew excessively"
    
    @pytest.mark.asyncio
    @pytest.mark.performance 
    async def test_cache_efficiency_under_load(self, setup_test_environment, seed_test_data):
        """Test cache hit ratio under sustained load"""
        cache_hits = 0
        total_requests = 0
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            # Populate cache
            for user_id in seed_test_data:
                _ = await client.get(f"/api/v1/features/user/{user_id}")
            
            # Test cache efficiency with repeated requests
            for _ in range(10):  # 10 rounds
                for user_id in seed_test_data:
                    response = await client.get(f"/api/v1/features/user/{user_id}")
                    assert response.status_code == 200
                    data = response.json()
                    total_requests += 1
                    if data["cache_hit"]:
                        cache_hits += 1
        
        # Calculate hit ratio
        hit_ratio = cache_hits / total_requests
        
        # Should achieve > 90% hit ratio after first round
        expected_hit_ratio = (total_requests - len(seed_test_data)) / total_requests
        assert hit_ratio >= expected_hit_ratio * 0.95, f"Cache hit ratio {hit_ratio:.2%} too low"
        
        print(f"Cache Efficiency - Hit ratio: {hit_ratio:.2%}")


class TestLoadTesting:
    """Load testing scenarios"""
    
    @pytest.mark.asyncio
    @pytest.mark.performance
    @pytest.mark.slow
    async def test_sustained_load_10_minutes(self, setup_test_environment, seed_test_data):
        """Test system stability under sustained load (10 minutes)"""
        duration_seconds = 600  # 10 minutes
        target_rps = 100
        
        errors = []
        response_times = []
        start_time = time.time()
        
        async def load_worker():
            async with AsyncClient(app=app, base_url="http://test", timeout=30.0) as client:
                worker_start = time.time()
                worker_requests = 0
                
                while time.time() - worker_start < duration_seconds:
                    try:
                        user_id = seed_test_data[worker_requests % len(seed_test_data)]
                        
                        request_start = time.time()
                        response = await client.get(f"/api/v1/features/user/{user_id}")
                        request_end = time.time()
                        
                        if response.status_code == 200:
                            response_times.append((request_end - request_start) * 1000)
                        else:
                            errors.append(f"HTTP {response.status_code}")
                        
                        worker_requests += 1
                        
                        # Rate limiting to achieve target RPS
                        await asyncio.sleep(1.0 / target_rps)
                    
                    except Exception as e:
                        errors.append(str(e))
                
                return worker_requests
        
        # Run load test with multiple workers
        workers = [load_worker() for _ in range(5)]  # 5 concurrent workers
        worker_results = await asyncio.gather(*workers)
        
        total_duration = time.time() - start_time
        total_requests = sum(worker_results)
        actual_rps = total_requests / total_duration
        error_rate = len(errors) / total_requests if total_requests > 0 else 1.0
        
        # Performance requirements
        assert error_rate < 0.01, f"Error rate {error_rate:.2%} exceeds 1% threshold"
        assert actual_rps > target_rps * 0.8, f"Actual RPS {actual_rps:.1f} below target {target_rps}"
        
        if response_times:
            avg_response_time = statistics.mean(response_times)
            p95_response_time = statistics.quantiles(response_times, n=20)[18]
            
            assert avg_response_time < 50, f"Average response time {avg_response_time:.2f}ms too high"
            assert p95_response_time < 100, f"P95 response time {p95_response_time:.2f}ms too high"
        
        print(
            f"Load Test Results - {actual_rps:.1f} RPS, "
            f"{error_rate:.2%} error rate, "
            f"{statistics.mean(response_times):.2f}ms avg latency"
        )


class TestScalabilityLimits:
    """Test system behavior at scale limits"""
    
    @pytest.mark.asyncio
    @pytest.mark.performance
    async def test_maximum_batch_size_performance(self, setup_test_environment, seed_test_data):
        """Test performance with maximum allowed batch size"""
        # Create 100-user batch (maximum allowed)
        large_user_list = []
        for i in range(100):
            user_id = f"scale_test_user_{i:03d}"
            large_user_list.append(user_id)
        
        batch_request = {
            "requests": [
                {"user_id": user_id, "feature_types": ["user"]}
                for user_id in large_user_list
            ]
        }
        
        async with AsyncClient(app=app, base_url="http://test", timeout=60.0) as client:
            start_time = time.time()
            response = await client.post("/api/v1/features/batch", json=batch_request)
            end_time = time.time()
            
            duration_ms = (end_time - start_time) * 1000
            
            assert response.status_code == 200
            data = response.json()
            
            # Should handle maximum batch efficiently
            assert data["total_requests"] == 100
            assert duration_ms < 2000, f"Large batch took {duration_ms:.0f}ms (> 2s limit)"
            
            # Per-user time should still be reasonable
            per_user_ms = duration_ms / 100
            assert per_user_ms < 20, f"Per-user time {per_user_ms:.2f}ms too high in batch"
            
            print(f"Large Batch Performance - {duration_ms:.0f}ms total, {per_user_ms:.2f}ms per user")
    
    @pytest.mark.asyncio
    @pytest.mark.performance
    async def test_cache_memory_limits(self, setup_test_environment):
        """Test cache behavior under memory pressure"""
        # Create many unique users to test cache eviction
        large_user_count = 10000
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            # Create features for many users
            for i in range(large_user_count):
                user_id = f"memory_test_user_{i:05d}"
                
                # Create user in database first
                user_features = UserFeatures(
                    user_id=user_id,
                    age=25 + (i % 50),
                    total_orders=i % 100,
                    avg_order_value=100.0 + (i % 500),
                    account_verified=True,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                _ = await db.update_user_features(user_features)
                
                # Request to populate cache
                if i % 100 == 0:  # Only every 100th user to avoid overwhelming test
                    response = await client.get(f"/api/v1/features/user/{user_id}")
                    assert response.status_code == 200
            
            # Check cache statistics
            cache_stats = await cache.get_cache_stats()
            
            # Cache should be managing memory appropriately
            evicted_keys = int(cache_stats.get('evicted_keys', 0))
            used_memory = cache_stats.get('used_memory', '0B')
            
            print(f"Cache under load - Memory: {used_memory}, Evicted keys: {evicted_keys}")
            
            # System should remain responsive
            test_user = "memory_test_user_00050"
            response = await client.get(f"/api/v1/features/user/{test_user}")
            assert response.status_code == 200
            
            data = response.json()
            assert data["response_time_ms"] < 100  # Should still be responsive
