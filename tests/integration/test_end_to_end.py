"""
Integration tests for end-to-end pipeline functionality
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from httpx import AsyncClient
from unittest.mock import AsyncMock

from src.main import app
from src.feature_store.cache import cache
from src.feature_store.database import db
from src.feature_store.models import UserFeatures


@pytest.fixture
async def setup_test_environment():
    """Setup test environment with database and cache connections"""
    await db.init()
    await cache.init()
    yield
    await db.close()
    await cache.close()


@pytest.fixture
async def seed_test_data():
    """Seed test data into database"""
    test_users = []
    for i in range(5):
        user_id = f"e2e_test_user_{i}"
        user_features = UserFeatures(
            user_id=user_id,
            age=25 + i,
            location_country="AE",
            location_city="Dubai",
            total_orders=10 + i * 5,
            avg_order_value=200.0 + i * 50,
            days_since_first_order=30 + i * 10,
            preferred_payment_method="bnpl",
            account_verified=True,
            created_at=datetime.utcnow() - timedelta(days=i),
            updated_at=datetime.utcnow() - timedelta(minutes=i * 10)
        )
        await db.update_user_features(user_features)
        test_users.append(user_id)
    return test_users


class TestEndToEndAPI:

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_complete_feature_retrieval_flow(self, setup_test_environment, seed_test_data):
        test_users = seed_test_data
        user_id = test_users[0]

        async with AsyncClient(app=app, base_url="http://test") as client:
            response1 = await client.get(f"/api/v1/features/user/{user_id}")
            assert response1.status_code == 200
            data1 = response1.json()
            assert data1["user_id"] == user_id
            assert not data1["cache_hit"]
            assert data1["user_features"]["age"] == 25

            response2 = await client.get(f"/api/v1/features/user/{user_id}")
            assert response2.status_code == 200
            data2 = response2.json()
            assert data2["cache_hit"]
            assert data2["response_time_ms"] < data1["response_time_ms"]

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_batch_feature_retrieval_flow(self, setup_test_environment, seed_test_data):
        batch_request = {
            "requests": [{"user_id": uid, "feature_types": ["user"]} for uid in seed_test_data[:3]]
        }
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.post("/api/v1/features/batch", json=batch_request)
            assert response.status_code == 200
            data = response.json()
            assert data["total_requests"] == 3
            assert data["successful_requests"] == 3
            assert len(data["responses"]) == 3
            for i, resp in enumerate(data["responses"]):
                assert resp["user_id"] == seed_test_data[i]
                assert resp["user_features"]["age"] == 25 + i

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_cache_invalidation_flow(self, setup_test_environment, seed_test_data):
        user_id = seed_test_data[0]
        async with AsyncClient(app=app, base_url="http://test") as client:
            await client.get(f"/api/v1/features/user/{user_id}")
            response2 = await client.get(f"/api/v1/features/user/{user_id}")
            assert response2.json()["cache_hit"]

            await client.delete(f"/api/v1/features/user/{user_id}")
            response3 = await client.get(f"/api/v1/features/user/{user_id}")
            assert not response3.json()["cache_hit"]

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_concurrent_api_requests(self, setup_test_environment, seed_test_data):
        async def make_request(client, user_id):
            return await client.get(f"/api/v1/features/user/{user_id}")

        async with AsyncClient(app=app, base_url="http://test") as client:
            tasks = [make_request(client, seed_test_data[i % len(seed_test_data)]) for i in range(20)]
            responses = await asyncio.gather(*tasks)
            for response in responses:
                assert response.status_code == 200


class TestEndToEndGRPC:

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_grpc_feature_retrieval(self, setup_test_environment, seed_test_data):
        from src.proto import feature_store_pb2
        from src.feature_store.grpc_service import FeatureStoreServicer

        servicer = FeatureStoreServicer()

        request = feature_store_pb2.UserFeatureRequest(
            user_id=seed_test_data[0],
            feature_types=["user"],
            include_metadata=True
        )
        context = AsyncMock()
        response = await servicer.GetUserFeatures(request, context)

        assert response.uid == seed_test_data[0]
        assert response.demographics.age == 25

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_grpc_batch_retrieval(self, setup_test_environment, seed_test_data):
        from src.proto import feature_store_pb2
        from src.feature_store.grpc_service import FeatureStoreServicer

        servicer = FeatureStoreServicer()
        user_requests = [
            feature_store_pb2.UserFeatureRequest(user_id=uid, feature_types=["user"])
            for uid in seed_test_data[:3]
        ]
        batch_request = feature_store_pb2.BatchFeatureRequest(requests=user_requests)
        context = AsyncMock()
        response = await servicer.GetBatchFeatures(batch_request, context)
        assert response.total_requests == 3
        assert response.successful_requests == 3
        assert len(response.responses) == 3

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_grpc_health_check(self, setup_test_environment):
        from src.proto import feature_store_pb2
        from src.feature_store.grpc_service import FeatureStoreServicer

        servicer = FeatureStoreServicer()
        request = feature_store_pb2.HealthCheckRequest(service="feature_store")
        context = AsyncMock()
        response = await servicer.HealthCheck(request, context)
        assert response.status in ["healthy", "unhealthy"]
