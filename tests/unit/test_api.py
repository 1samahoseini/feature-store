"""
Unit tests for FastAPI REST endpoints
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from httpx import AsyncClient

from src.main import app
from src.feature_store.models import FeatureResponse, UserFeatures
from datetime import datetime


@pytest.fixture
def mock_feature_store():
    """Mock feature store for testing"""
    with patch('src.feature_store.api.feature_store') as mock:
        yield mock


@pytest.fixture
def mock_cache():
    """Mock cache for testing"""
    with patch('src.feature_store.api.cache') as mock:
        yield mock


@pytest.fixture
def sample_user_features():
    """Sample user features for testing"""
    return UserFeatures(
        user_id="test_user_123",
        age=28,
        location_country="US",
        location_city="New York",
        total_orders=15,
        avg_order_value=125.50,
        days_since_first_order=45,
        preferred_payment_method="credit_card",
        account_verified=True,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )


@pytest.fixture
def sample_feature_response(sample_user_features):
    """Sample feature response for testing"""
    return FeatureResponse(
        user_id="test_user_123",
        user_features=sample_user_features,
        transaction_features=None,
        risk_features=None,
        response_time_ms=32.4,
        cache_hit=True,
        data_freshness_minutes=15
    )


class TestFeatureAPI:
    """Test cases for feature API endpoints"""
    
    @pytest.mark.asyncio
    async def test_get_user_features_success(self, mock_feature_store, sample_feature_response):
        """Test successful user features retrieval"""
        mock_feature_store.get_features = AsyncMock(return_value=sample_feature_response)
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get("/api/v1/features/user/test_user_123")
        
        assert response.status_code == 200
        data = response.json()
        assert data["user_id"] == "test_user_123"
        assert data["cache_hit"]
        assert data["response_time_ms"] == 32.4
        assert "user_features" in data
        
        mock_feature_store.get_features.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_user_features_with_params(self, mock_feature_store, sample_feature_response):
        """Test user features retrieval with query parameters"""
        mock_feature_store.get_features = AsyncMock(return_value=sample_feature_response)
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get(
                "/api/v1/features/user/test_user_123",
                params={"feature_types": ["user", "transaction"], "include_metadata": True}
            )
        
        assert response.status_code == 200
        mock_feature_store.get_features.assert_called_once()
        
        # Verify the request object passed to feature store
        call_args = mock_feature_store.get_features.call_args[0][0]
        assert call_args.user_id == "test_user_123"
        assert "user" in call_args.feature_types
        assert "transaction" in call_args.feature_types
        assert call_args.include_metadata
    
    @pytest.mark.asyncio
    async def test_get_user_features_invalid_user_id(self, mock_feature_store):
        """Test user features retrieval with invalid user ID"""
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get("/api/v1/features/user/")
        
        assert response.status_code == 404
        mock_feature_store.get_features.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_user_features_empty_user_id(self, mock_feature_store):
        """Test user features retrieval with empty user ID"""
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get("/api/v1/features/user/")
        
        assert response.status_code == 404
    
    @pytest.mark.asyncio
    async def test_get_user_features_server_error(self, mock_feature_store):
        """Test server error handling"""
        mock_feature_store.get_features = AsyncMock(side_effect=Exception("Database error"))
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get("/api/v1/features/user/test_user_123")
        
        assert response.status_code == 500
        data = response.json()
        assert "error" in data
    
    @pytest.mark.asyncio
    async def test_batch_features_success(self, mock_feature_store):
        """Test successful batch features retrieval"""
        from src.feature_store.models import BatchFeatureResponse
        
        batch_response = BatchFeatureResponse(
            responses=[sample_feature_response()],
            total_requests=2,
            successful_requests=2,
            failed_requests=0,
            total_response_time_ms=64.8,
            cache_hit_ratio=0.5
        )
        
        mock_feature_store.get_batch_features = AsyncMock(return_value=batch_response)
        
        request_data = {
            "requests": [
                {"user_id": "user1", "feature_types": ["user"]},
                {"user_id": "user2", "feature_types": ["user", "transaction"]}
            ]
        }
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/features/batch",
                json=request_data
            )
        
        assert response.status_code == 200
        data = response.json()
        assert data["total_requests"] == 2
        assert data["successful_requests"] == 2
        assert data["cache_hit_ratio"] == 0.5
        
        mock_feature_store.get_batch_features.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_batch_features_too_large(self, mock_feature_store):
        """Test batch request size limit"""
        # Create request with 101 users (over the 100 limit)
        request_data = {
            "requests": [{"user_id": f"user{i}", "feature_types": ["user"]} for i in range(101)]
        }
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/features/batch",
                json=request_data
            )
        
        assert response.status_code == 400
        data = response.json()
        assert "too large" in data["detail"].lower()
        mock_feature_store.get_batch_features.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_invalidate_cache_success(self, mock_cache):
        """Test successful cache invalidation"""
        mock_cache.delete_user_features = AsyncMock(return_value=True)
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.delete("/api/v1/features/user/test_user_123")
        
        assert response.status_code == 200
        data = response.json()
        assert "invalidated" in data["message"].lower()
        
        mock_cache.delete_user_features.assert_called_once_with("test_user_123")
    
    @pytest.mark.asyncio
    async def test_invalidate_cache_failure(self, mock_cache):
        """Test cache invalidation failure"""
        mock_cache.delete_user_features = AsyncMock(return_value=False)
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.delete("/api/v1/features/user/test_user_123")
        
        assert response.status_code == 500
        data = response.json()
        assert "failed" in data["detail"].lower()
    
    @pytest.mark.asyncio
    async def test_health_check(self, mock_cache):
        """Test health check endpoint"""
        with patch('src.feature_store.api.db') as mock_db:
            mock_cache.health_check = AsyncMock(return_value=True)
            mock_db.health_check = AsyncMock(return_value=True)
            
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.get("/api/v1/features/health")
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "healthy"
            assert data["checks"]["cache"]
            assert data["checks"]["database"]
            assert data["checks"]["overall"]
    
    @pytest.mark.asyncio
    async def test_health_check_unhealthy(self, mock_cache):
        """Test health check with unhealthy components"""
        with patch('src.feature_store.api.db') as mock_db:
            mock_cache.health_check = AsyncMock(return_value=False)
            mock_db.health_check = AsyncMock(return_value=True)
            
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.get("/api/v1/features/health")
            
            assert response.status_code == 503
            data = response.json()
            assert data["status"] == "unhealthy"
            assert not data["checks"]["cache"]
            assert not data["checks"]["overall"]
    
    @pytest.mark.asyncio
    async def test_get_stats(self, mock_cache):
        """Test statistics endpoint"""
        mock_cache_stats = {
            "keyspace_hits": 1000,
            "keyspace_misses": 200,
            "used_memory": "50MB"
        }
        
        mock_db_stats = {
            "user_features_count": 50000,
            "transaction_features_count": 45000,
            "database_size_bytes": 1073741824
        }
        
        with patch('src.feature_store.api.db') as mock_db:
            mock_cache.get_cache_stats = AsyncMock(return_value=mock_cache_stats)
            mock_db.get_database_stats = AsyncMock(return_value=mock_db_stats)
            
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.get("/api/v1/features/stats")
            
            assert response.status_code == 200
            data = response.json()
            assert "cache" in data
            assert "database" in data
            assert data["cache"]["keyspace_hits"] == 1000
            assert data["database"]["user_features_count"] == 50000
    
    @pytest.mark.asyncio
    async def test_refresh_features(self, mock_cache):
        """Test feature refresh endpoint"""
        mock_cache.delete_user_features = AsyncMock(return_value=True)
        
        request_data = ["user1", "user2", "user3"]
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/features/refresh",
                json=request_data
            )
        
        assert response.status_code == 200
        data = response.json()
        assert data["successful"] == 3
        assert data["requested"] == 3
        
        # Verify cache deletion was called for each user
        assert mock_cache.delete_user_features.call_count == 3
    
    @pytest.mark.asyncio
    async def test_refresh_features_too_many(self, mock_cache):
        """Test feature refresh with too many users"""
        # Create request with 1001 users (over the 1000 limit)
        request_data = [f"user{i}" for i in range(1001)]
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/features/refresh",
                json=request_data
            )
        
        assert response.status_code == 400
        data = response.json()
        assert "too many" in data["detail"].lower()
        mock_cache.delete_user_features.assert_not_called()


class TestAPIValidation:
    """Test input validation and error handling"""
    
    @pytest.mark.asyncio
    async def test_invalid_feature_types(self, mock_feature_store):
        """Test invalid feature types parameter"""
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get(
                "/api/v1/features/user/test_user",
                params={"feature_types": ["invalid_type"]}
            )
        
        assert response.status_code == 422  # Validation error
    
    @pytest.mark.asyncio
    async def test_malformed_batch_request(self, mock_feature_store):
        """Test malformed batch request"""
        request_data = {
            "invalid_field": "invalid_value"
        }
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/features/batch",
                json=request_data
            )
        
        assert response.status_code == 422  # Validation error
    
    @pytest.mark.asyncio
    async def test_empty_batch_request(self, mock_feature_store):
        """Test empty batch request"""
        request_data = {"requests": []}
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.post(
                "/api/v1/features/batch",
                json=request_data
            )
        
        assert response.status_code == 422  # Validation error


class TestAPIPerformance:
    """Test API performance characteristics"""
    
    @pytest.mark.asyncio
    async def test_response_time_logging(self, mock_feature_store, sample_feature_response):
        """Test that response times are properly logged"""
        mock_feature_store.get_features = AsyncMock(return_value=sample_feature_response)
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get("/api/v1/features/user/test_user_123")
        
        assert response.status_code == 200
        data = response.json()
        assert "response_time_ms" in data
        assert isinstance(data["response_time_ms"], (int, float))
        assert data["response_time_ms"] > 0
    
    @pytest.mark.asyncio
    async def test_concurrent_requests(self, mock_feature_store, sample_feature_response):
        """Test handling of concurrent requests"""
        mock_feature_store.get_features = AsyncMock(return_value=sample_feature_response)
        
        async def make_request(client, user_id):
            return await client.get(f"/api/v1/features/user/{user_id}")
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            # Make 10 concurrent requests
            tasks = [make_request(client, f"user_{i}") for i in range(10)]
            responses = await asyncio.gather(*tasks)
        
        # All requests should succeed
        for response in responses:
            assert response.status_code == 200
        
        # Mock should have been called 10 times
        assert mock_feature_store.get_features.call_count == 10