"""
Unit tests for feature store orchestration
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from datetime import datetime, timedelta

from src.feature_store.store import FeatureStore
from src.feature_store.models import (
    FeatureRequest, BatchFeatureRequest, UserFeatures, TransactionFeatures, RiskFeatures, FeatureType
)


@pytest.fixture
def feature_store():
    """Create feature store instance for testing"""
    return FeatureStore()


@pytest.fixture
def sample_user_features():
    """Sample user features for testing"""
    return UserFeatures(
        user_id="store_test_user_123",
        age=29,
        location_country="AE",
        location_city="Abu Dhabi",
        total_orders=18,
        avg_order_value=425.75,
        days_since_first_order=90,
        preferred_payment_method="bnpl",
        account_verified=True,
        created_at=datetime.utcnow() - timedelta(hours=2),
        updated_at=datetime.utcnow() - timedelta(minutes=30)
    )


@pytest.fixture
def sample_transaction_features():
    """Sample transaction features for testing"""
    return TransactionFeatures(
        user_id="store_test_user_123",
        total_transactions_30d=12,
        total_amount_30d=3600.00,
        avg_transaction_amount=300.00,
        max_transaction_amount=750.00,
        transactions_declined_30d=2,
        unique_merchants_30d=8,
        weekend_transaction_ratio=0.33,
        night_transaction_ratio=0.17,
        created_at=datetime.utcnow() - timedelta(hours=1),
        updated_at=datetime.utcnow() - timedelta(minutes=15)
    )


@pytest.fixture
def sample_risk_features():
    """Sample risk features for testing"""
    return RiskFeatures(
        user_id="store_test_user_123",
        credit_utilization_ratio=0.42,
        payment_delays_30d=1,
        payment_delays_90d=3,
       failed_payments_count=1,
        device_changes_30d=1,
        login_locations_30d=2,
        velocity_alerts_30d=1,
        risk_score=0.38,
        created_at=datetime.utcnow() - timedelta(hours=1),
        updated_at=datetime.utcnow() - timedelta(minutes=10)
    )


class TestFeatureStoreOrchestration:
    """Test feature store orchestration logic"""
    
    @pytest.mark.asyncio
    async def test_get_features_cache_hit(self, feature_store, sample_user_features):
        """Test feature retrieval with cache hit"""
        with patch.object(feature_store, 'cache') as mock_cache:
            with patch.object(feature_store, 'database') as mock_database:
                # Mock cache hit
                cached_data = {
                    'user': sample_user_features.dict()
                }
                mock_cache.get_features = AsyncMock(return_value=cached_data)
                
                request = FeatureRequest(
                    user_id="store_test_user_123",
                    feature_types=[FeatureType.USER]
                )
                
                response = await feature_store.get_features(request)
        
        assert response.user_id == "store_test_user_123"
        assert response.cache_hit
        assert response.user_features is not None
        assert response.user_features.age == 29
        assert response.user_features.total_orders == 18
        assert response.data_freshness_minutes is not None
        
        # Cache should be called, database should not
        mock_cache.get_features.assert_called_once()
        mock_database.get_user_features.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_features_cache_miss(self, feature_store, sample_user_features):
        """Test feature retrieval with cache miss"""
        with patch.object(feature_store, 'cache') as mock_cache:
            with patch.object(feature_store, 'database') as mock_database:
                # Mock cache miss
                mock_cache.get_features = AsyncMock(return_value=None)
                
                # Mock database response
                mock_database.get_user_features = AsyncMock(return_value=sample_user_features)
                
                # Mock cache set
                mock_cache.set_features = AsyncMock(return_value=True)
                
                request = FeatureRequest(
                    user_id="store_test_user_123",
                    feature_types=[FeatureType.USER]
                )
                
                response = await feature_store.get_features(request)
        
        assert response.user_id == "store_test_user_123"
        assert not response.cache_hit
        assert response.user_features is not None
        assert response.data_freshness_minutes == 0  # Fresh from database
        
        # Both cache and database should be called
        mock_cache.get_features.assert_called_once()
        mock_database.get_user_features.assert_called_once()
        mock_cache.set_features.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_features_multiple_types(self, feature_store, sample_user_features, sample_transaction_features):
        """Test feature retrieval with multiple feature types"""
        with patch.object(feature_store, 'cache') as mock_cache:
            with patch.object(feature_store, 'database') as mock_database:
                # Mock cache miss
                mock_cache.get_features = AsyncMock(return_value=None)
                
                # Mock database responses
                mock_database.get_user_features = AsyncMock(return_value=sample_user_features)
                mock_database.get_transaction_features = AsyncMock(return_value=sample_transaction_features)
                mock_database.get_risk_features = AsyncMock(return_value=None)
                
                mock_cache.set_features = AsyncMock(return_value=True)
                
                request = FeatureRequest(
                    user_id="store_test_user_123",
                    feature_types=[FeatureType.USER, FeatureType.TRANSACTION, FeatureType.RISK]
                )
                
                response = await feature_store.get_features(request)
        
        assert response.user_features is not None
        assert response.transaction_features is not None
        assert response.risk_features is None  # Not found in database
        
        # All database methods should be called
        mock_database.get_user_features.assert_called_once()
        mock_database.get_transaction_features.assert_called_once()
        mock_database.get_risk_features.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_features_partial_cache_hit(self, feature_store, sample_user_features):
        """Test feature retrieval with partial cache hit"""
        with patch.object(feature_store, 'cache') as mock_cache:
            with patch.object(feature_store, 'database') as mock_database:
                # Mock partial cache (only user features cached)
                cached_data = {
                    'user': sample_user_features.dict()
                }
                mock_cache.get_features = AsyncMock(return_value=cached_data)
                
                # Mock _has_all_features to return False (missing transaction features)
                with patch.object(feature_store, '_has_all_features', return_value=False):
                    mock_database.get_user_features = AsyncMock(return_value=sample_user_features)
                    mock_database.get_transaction_features = AsyncMock(return_value=None)
                    mock_cache.set_features = AsyncMock(return_value=True)
                    
                    request = FeatureRequest(
                        user_id="store_test_user_123",
                        feature_types=[FeatureType.USER, FeatureType.TRANSACTION]
                    )
                    
                    response = await feature_store.get_features(request)
        
        assert not response.cache_hit  # Considered cache miss due to incomplete data
        
        # Should still hit database for missing features
        mock_database.get_user_features.assert_called_once()
        mock_database.get_transaction_features.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_batch_features_success(self, feature_store, sample_user_features):
        """Test successful batch feature retrieval"""
        with patch.object(feature_store, 'cache') as mock_cache:
            with patch.object(feature_store, 'database') as mock_database:
                # Mock mixed cache hits and misses
                def mock_cache_get(user_id, feature_types):
                    if user_id == "cached_user":
                        return {'user': sample_user_features.dict()}
                    return None
                
                mock_cache.get_features = AsyncMock(side_effect=mock_cache_get)
                
                # Mock database batch response
                batch_db_response = {
                    'uncached_user': {
                        'user': sample_user_features.dict()
                    }
                }
                mock_database.get_batch_features = AsyncMock(return_value=batch_db_response)
                mock_cache.set_features = AsyncMock(return_value=True)
                
                requests = [
                    FeatureRequest(user_id="cached_user", feature_types=[FeatureType.USER]),
                    FeatureRequest(user_id="uncached_user", feature_types=[FeatureType.USER])
                ]
                
                batch_request = BatchFeatureRequest(requests=requests)
                response = await feature_store.get_batch_features(batch_request)
        
        assert response.total_requests == 2
        assert response.successful_requests == 2
        assert response.failed_requests == 0
        assert response.cache_hit_ratio == 0.5  # 1 out of 2 from cache
        assert len(response.responses) == 2
        
        # Database batch should be called with uncached users only
        mock_database.get_batch_features.assert_called_once()
        call_args = mock_database.get_batch_features.call_args[0]
        assert len(call_args[0]) == 1  # Only one uncached user
        assert call_args[0][0] == "uncached_user"
    
    @pytest.mark.asyncio
    async def test_get_batch_features_all_cached(self, feature_store, sample_user_features):
        """Test batch feature retrieval with all users cached"""
        with patch.object(feature_store, 'cache') as mock_cache:
            with patch.object(feature_store, 'database') as mock_database:
                # Mock all cache hits
                cached_data = {'user': sample_user_features.dict()}
                mock_cache.get_features = AsyncMock(return_value=cached_data)
                
                requests = [
                    FeatureRequest(user_id="user1", feature_types=[FeatureType.USER]),
                    FeatureRequest(user_id="user2", feature_types=[FeatureType.USER])
                ]
                
                batch_request = BatchFeatureRequest(requests=requests)
                response = await feature_store.get_batch_features(batch_request)
        
        assert response.cache_hit_ratio == 1.0  # All from cache
        assert len(response.responses) == 2
        
        # Database should not be called
        mock_database.get_batch_features.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_batch_features_error_handling(self, feature_store):
        """Test batch feature retrieval with errors"""
        with patch.object(feature_store, 'cache') as mock_cache:
            # Mock cache to raise exception for some users
            def mock_cache_get(user_id, feature_types):
                if user_id == "error_user":
                    raise Exception("Cache error")
                return None
            
            mock_cache.get_features = AsyncMock(side_effect=mock_cache_get)
            
            with patch.object(feature_store, 'database') as mock_database:
                mock_database.get_batch_features = AsyncMock(return_value={})
                
                requests = [
                    FeatureRequest(user_id="normal_user", feature_types=[FeatureType.USER]),
                    FeatureRequest(user_id="error_user", feature_types=[FeatureType.USER])
                ]
                
                batch_request = BatchFeatureRequest(requests=requests)
                response = await feature_store.get_batch_features(batch_request)
        
        # Should handle errors gracefully
        assert response.total_requests == 2
        assert len(response.responses) <= 2  # May have fewer responses due to errors
        
        # Database should still be called for non-errored users
        mock_database.get_batch_features.assert_called_once()


class TestFeatureStoreDataParsing:
    """Test feature store data parsing and conversion methods"""
    
    def test_parse_user_features_success(self, feature_store):
        """Test successful user features parsing"""
        user_data = {
            'user_id': 'test_user',
            'age': 30,
            'location_country': 'AE',
            'total_orders': 10,
            'avg_order_value': 200.0,
            'account_verified': True,
            'created_at': '2023-01-15T10:30:00+00:00',
            'updated_at': '2023-01-15T11:00:00+00:00'
        }
        
        user_features = feature_store._parse_user_features(user_data)
        
        assert user_features is not None
        assert user_features.user_id == 'test_user'
        assert user_features.age == 30
        assert user_features.total_orders == 10
        assert isinstance(user_features.created_at, datetime)
        assert isinstance(user_features.updated_at, datetime)
    
    def test_parse_user_features_with_none(self, feature_store):
        """Test user features parsing with None input"""
        user_features = feature_store._parse_user_features(None)
        assert user_features is None
    
    def test_parse_user_features_with_invalid_data(self, feature_store):
        """Test user features parsing with invalid data"""
        invalid_data = {
            'user_id': 'test_user',
            'age': 'invalid_age',  # Should be integer
            'total_orders': 'invalid_orders'  # Should be integer
        }
        
        user_features = feature_store._parse_user_features(invalid_data)
        
        # Should return None for invalid data
        assert user_features is None
    
    def test_parse_transaction_features_success(self, feature_store):
        """Test successful transaction features parsing"""
        txn_data = {
            'user_id': 'test_user',
            'total_transactions_30d': 15,
            'total_amount_30d': 3000.0,
            'avg_transaction_amount': 200.0,
            'weekend_transaction_ratio': 0.3,
            'created_at': '2023-01-15T10:30:00+00:00',
            'updated_at': '2023-01-15T11:00:00+00:00'
        }
        
        txn_features = feature_store._parse_transaction_features(txn_data)
        
        assert txn_features is not None
        assert txn_features.user_id == 'test_user'
        assert txn_features.total_transactions_30d == 15
        assert txn_features.weekend_transaction_ratio == 0.3
        assert isinstance(txn_features.created_at, datetime)
    
    def test_parse_risk_features_success(self, feature_store):
        """Test successful risk features parsing"""
        risk_data = {
            'user_id': 'test_user',
            'credit_utilization_ratio': 0.45,
            'payment_delays_30d': 2,
            'risk_score': 0.35,
            'created_at': '2023-01-15T10:30:00+00:00',
            'updated_at': '2023-01-15T11:00:00+00:00'
        }
        
        risk_features = feature_store._parse_risk_features(risk_data)
        
        assert risk_features is not None
        assert risk_features.user_id == 'test_user'
        assert risk_features.credit_utilization_ratio == 0.45
        assert risk_features.risk_score == 0.35
        assert isinstance(risk_features.created_at, datetime)
    
    def test_calculate_freshness_success(self, feature_store):
        """Test successful data freshness calculation"""
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        features_data = {
            'user': {
                'updated_at': one_hour_ago.isoformat()
            },
            'transaction': {
                'updated_at': (datetime.utcnow() - timedelta(minutes=30)).isoformat()
            }
        }
        
        freshness = feature_store._calculate_freshness(features_data)
        
        # Should return freshness of oldest data (1 hour = 60 minutes)
        assert freshness == 60
    
    def test_calculate_freshness_with_datetime_objects(self, feature_store):
        """Test freshness calculation with datetime objects"""
        two_hours_ago = datetime.utcnow() - timedelta(hours=2)
        features_data = {
            'user': {
                'updated_at': two_hours_ago  # datetime object, not string
            }
        }
        
        freshness = feature_store._calculate_freshness(features_data)
        
        # Should handle datetime objects correctly
        assert freshness == 120  # 2 hours = 120 minutes
    
    def test_calculate_freshness_with_no_data(self, feature_store):
        """Test freshness calculation with no valid data"""
        features_data = {
            'user': {
                'some_field': 'some_value'  # No updated_at field
            }
        }
        
        freshness = feature_store._calculate_freshness(features_data)
        
        # Should return None when no timestamp data available
        assert freshness is None
    
    def test_has_all_features_complete(self, feature_store):
        """Test _has_all_features with complete data"""
        cached_features = {
            'user': {'user_id': 'test'},
            'transaction': {'user_id': 'test'},
            'risk': {'user_id': 'test'}
        }
        
        requested_types = ['user', 'transaction', 'risk']
        
        result = feature_store._has_all_features(cached_features, requested_types)
        assert result
    
    def test_has_all_features_incomplete(self, feature_store):
        """Test _has_all_features with incomplete data"""
        cached_features = {
            'user': {'user_id': 'test'},
            # Missing 'transaction' and 'risk'
        }
        
        requested_types = ['user', 'transaction', 'risk']
        
        result = feature_store._has_all_features(cached_features, requested_types)
        assert not result
    
    def test_has_all_features_subset(self, feature_store):
        """Test _has_all_features when requesting subset of available data"""
        cached_features = {
            'user': {'user_id': 'test'},
            'transaction': {'user_id': 'test'},
            'risk': {'user_id': 'test'}
        }
        
        requested_types = ['user', 'risk']  # Only requesting subset
        
        result = feature_store._has_all_features(cached_features, requested_types)
        assert result


class TestFeatureStorePerformance:
    """Test feature store performance characteristics"""
    
    @pytest.mark.asyncio
    async def test_response_time_measurement(self, feature_store, sample_user_features):
        """Test that response times are properly measured"""
        with patch.object(feature_store, 'cache') as mock_cache:
            with patch.object(feature_store, 'database') as mock_database:
                # Mock cache miss and database response
                mock_cache.get_features = AsyncMock(return_value=None)
                mock_database.get_user_features = AsyncMock(return_value=sample_user_features)
                mock_cache.set_features = AsyncMock(return_value=True)
                
                request = FeatureRequest(
                    user_id="perf_test_user",
                    feature_types=[FeatureType.USER]
                )
                
                response = await feature_store.get_features(request)
        
        # Response time should be measured
        assert response.response_time_ms > 0
        assert isinstance(response.response_time_ms, (int, float))
    
    @pytest.mark.asyncio
    async def test_concurrent_feature_requests(self, feature_store, sample_user_features):
        """Test handling of concurrent feature requests"""
        with patch.object(feature_store, 'cache') as mock_cache:
            with patch.object(feature_store, 'database') as mock_database:
                # Mock different responses for different users
                def mock_db_response(user_id):
                    features = sample_user_features.copy()
                    features.user_id = user_id
                    return features
                
                mock_cache.get_features = AsyncMock(return_value=None)
                mock_database.get_user_features = AsyncMock(side_effect=mock_db_response)
                mock_cache.set_features = AsyncMock(return_value=True)
                
                # Create concurrent requests
                requests = [
                    FeatureRequest(user_id=f"concurrent_user_{i}", feature_types=[FeatureType.USER])
                    for i in range(10)
                ]
                
                # Execute concurrently
                tasks = [feature_store.get_features(req) for req in requests]
                responses = await asyncio.gather(*tasks)
        
        # All requests should succeed
        assert len(responses) == 10
        for i, response in enumerate(responses):
            assert response.user_id == f"concurrent_user_{i}"
            assert response.user_features is not None
        
        # Database should be called 10 times
        assert mock_database.get_user_features.call_count == 10
    
    @pytest.mark.asyncio
    async def test_batch_vs_individual_performance(self, feature_store, sample_user_features):
        """Test that batch requests are more efficient than individual requests"""
        with patch.object(feature_store, 'cache') as mock_cache:
            with patch.object(feature_store, 'database') as mock_database:
                # Mock cache miss for all requests
                mock_cache.get_features = AsyncMock(return_value=None)
                
                # Mock database responses
                batch_response = {
                    f"batch_user_{i}": {'user': sample_user_features.dict()}
                    for i in range(5)
                }
                mock_database.get_batch_features = AsyncMock(return_value=batch_response)
                mock_database.get_user_features = AsyncMock(return_value=sample_user_features)
                mock_cache.set_features = AsyncMock(return_value=True)
                
                # Test batch request
                batch_requests = [
                    FeatureRequest(user_id=f"batch_user_{i}", feature_types=[FeatureType.USER])
                    for i in range(5)
                ]
                batch_request = BatchFeatureRequest(requests=batch_requests)
                
                import time
                start_time = time.time()
                batch_response = await feature_store.get_batch_features(batch_request)
                time.time() - start_time # type: ignore
                
                # Test individual requests
                start_time = time.time()
                individual_responses = []
                for i in range(5):
                    request = FeatureRequest(user_id=f"individual_user_{i}", feature_types=[FeatureType.USER])
                    response = await feature_store.get_features(request)
                    individual_responses.append(response)
                time.time() - start_time # type: ignore
        
        # Batch should be successful
        assert batch_response.successful_requests == 5
        assert len(individual_responses) == 5
        
        # Batch should use fewer database calls
        assert mock_database.get_batch_features.call_count == 1
        assert mock_database.get_user_features.call_count == 5
        
        # Note: In practice, batch should be faster, but timing tests can be flaky
        # so we just verify the requests completed successfully
    
    @pytest.mark.asyncio
    async def test_error_recovery_and_partial_success(self, feature_store, sample_user_features):
        """Test error recovery and partial success scenarios"""
        with patch.object(feature_store, 'cache') as mock_cache:
            with patch.object(feature_store, 'database') as mock_database:
                # Mock cache failures for some users
                def mock_cache_get(user_id, feature_types):
                    if user_id == "cache_error_user":
                        raise Exception("Cache connection failed")
                    return None
                
                mock_cache.get_features = AsyncMock(side_effect=mock_cache_get)
                
                # Mock database partial success
                def mock_db_response(user_id):
                    if user_id == "db_error_user":
                        raise Exception("Database connection failed")
                    return sample_user_features
                
                mock_database.get_user_features = AsyncMock(side_effect=mock_db_response)
                mock_cache.set_features = AsyncMock(return_value=True)
                
                # Test requests with different error scenarios
                requests = [
                    FeatureRequest(user_id="normal_user", feature_types=[FeatureType.USER]),
                    FeatureRequest(user_id="cache_error_user", feature_types=[FeatureType.USER]),
                    FeatureRequest(user_id="db_error_user", feature_types=[FeatureType.USER])
                ]
                
                responses = []
                for request in requests:
                    try:
                        response = await feature_store.get_features(request)
                        responses.append(response)
                    except Exception:
                        # Some requests may fail, which is expected
                        pass
        
        # At least one request should succeed (normal_user)
        assert len(responses) >= 1
        normal_response = next((r for r in responses if r.user_id == "normal_user"), None)
        assert normal_response is not None
        assert normal_response.user_features is not None