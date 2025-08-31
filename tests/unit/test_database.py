"""
Unit tests for database layer
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, timedelta

from src.feature_store.database import DatabaseManager
from src.feature_store.models import UserFeatures, TransactionFeatures, RiskFeatures


@pytest.fixture
def db_manager():
    """Create database manager instance for testing"""
    return DatabaseManager()


@pytest.fixture
def mock_asyncpg_pool():
    """Mock asyncpg connection pool"""
    mock_pool = MagicMock()
    mock_conn = MagicMock()
    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
    return mock_pool, mock_conn


@pytest.fixture
def sample_user_row():
    """Sample database row for user features"""
    return {
        'user_id': 'db_test_user_123',
        'age': 31,
        'location_country': 'AE',
        'location_city': 'Dubai',
        'total_orders': 22,
        'avg_order_value': 375.50,
        'days_since_first_order': 150,
        'preferred_payment_method': 'credit_card',
        'account_verified': True,
        'created_at': datetime.utcnow() - timedelta(days=5),
        'updated_at': datetime.utcnow() - timedelta(hours=2)
    }


class TestDatabaseManager:
    """Test database manager initialization and connection handling"""
    
    @pytest.mark.asyncio
    async def test_init_success(self, db_manager):
        """Test successful database initialization"""
        with patch('src.feature_store.database.asyncpg.create_pool') as mock_create_pool:
            mock_pool = MagicMock()
            mock_conn = MagicMock()
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
            mock_conn.fetchval = AsyncMock(return_value=1)
            
            mock_create_pool.return_value = mock_pool
            
            await db_manager.init()
            
            assert db_manager.pool is not None
            mock_create_pool.assert_called_once()
            mock_conn.fetchval.assert_called_once_with('SELECT 1')
    
    @pytest.mark.asyncio
    async def test_init_failure(self, db_manager):
        """Test database initialization failure"""
        with patch('src.feature_store.database.asyncpg.create_pool') as mock_create_pool:
            mock_create_pool.side_effect = Exception("Connection failed")
            
            with pytest.raises(Exception, match="Connection failed"):
                await db_manager.init()
            
            assert db_manager.pool is None
    
    @pytest.mark.asyncio
    async def test_close(self, db_manager):
        """Test database connection cleanup"""
        mock_pool = MagicMock()
        mock_pool.close = AsyncMock()
        db_manager.pool = mock_pool
        
        await db_manager.close()
        
        mock_pool.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_success(self, db_manager):
        """Test successful health check"""
        mock_pool, mock_conn = mock_asyncpg_pool()
        mock_conn.fetchval = AsyncMock(return_value=1)
        db_manager.pool = mock_pool
        
        result = await db_manager.health_check()
        
        assert result
        mock_conn.fetchval.assert_called_once_with('SELECT 1')
    
    @pytest.mark.asyncio
    async def test_health_check_failure(self, db_manager):
        """Test health check failure"""
        mock_pool, mock_conn = mock_asyncpg_pool()
        mock_conn.fetchval = AsyncMock(side_effect=Exception("DB error"))
        db_manager.pool = mock_pool
        
        result = await db_manager.health_check()
        
        assert not result
    
    @pytest.mark.asyncio
    async def test_health_check_no_pool(self, db_manager):
        """Test health check with no pool"""
        db_manager.pool = None
        
        result = await db_manager.health_check()
        
        assert not result


class TestUserFeatures:
    """Test user features database operations"""
    
    @pytest.mark.asyncio
    async def test_get_user_features_success(self, db_manager, sample_user_row):
        """Test successful user features retrieval"""
        mock_pool, mock_conn = mock_asyncpg_pool()
        mock_conn.fetchrow = AsyncMock(return_value=sample_user_row)
        db_manager.pool = mock_pool
        
        result = await db_manager.get_user_features('db_test_user_123')
        
        assert result is not None
        assert isinstance(result, UserFeatures)
        assert result.user_id == 'db_test_user_123'
        assert result.age == 31
        assert result.total_orders == 22
        assert result.account_verified
        
        # Verify correct query was executed
        mock_conn.fetchrow.assert_called_once()
        call_args = mock_conn.fetchrow.call_args[0]
        assert 'SELECT' in call_args[0]
        assert 'user_features' in call_args[0]
        assert call_args[1] == 'db_test_user_123'
    
    @pytest.mark.asyncio
    async def test_get_user_features_not_found(self, db_manager):
        """Test user features retrieval when user not found"""
        mock_pool, mock_conn = mock_asyncpg_pool()
        mock_conn.fetchrow = AsyncMock(return_value=None)
        db_manager.pool = mock_pool
        
        result = await db_manager.get_user_features('nonexistent_user')
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_user_features_database_error(self, db_manager):
        """Test user features retrieval with database error"""
        mock_pool, mock_conn = mock_asyncpg_pool()
        mock_conn.fetchrow = AsyncMock(side_effect=Exception("Database error"))
        db_manager.pool = mock_pool
        
        result = await db_manager.get_user_features('db_test_user_123')
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_update_user_features_success(self, db_manager):
        """Test successful user features update"""
        mock_pool, mock_conn = mock_asyncpg_pool()
        mock_conn.execute = AsyncMock()
        db_manager.pool = mock_pool
        
        user_features = UserFeatures(
            user_id="update_test_user",
            age=28,
            location_country="AE",
            location_city="Dubai",
            total_orders=10,
            avg_order_value=200.0,
            days_since_first_order=30,
            account_verified=True,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        result = await db_manager.update_user_features(user_features)
        
        assert result
        mock_conn.execute.assert_called_once()
        
        # Verify upsert query
        call_args = mock_conn.execute.call_args[0]
        assert 'INSERT INTO user_features' in call_args[0]
        assert 'ON CONFLICT (user_id) DO UPDATE' in call_args[0]
    
    @pytest.mark.asyncio
    async def test_update_user_features_failure(self, db_manager):
        """Test user features update failure"""
        mock_pool, mock_conn = mock_asyncpg_pool()
        mock_conn.execute = AsyncMock(side_effect=Exception("Update failed"))
        db_manager.pool = mock_pool
        
        user_features = UserFeatures(
            user_id="update_test_user",
            age=28,
            location_country="AE",
            location_city="Dubai",
            total_orders=10,
            avg_order_value=200.0,
            days_since_first_order=30,
            account_verified=True,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        result = await db_manager.update_user_features(user_features)
        
        assert not result


class TestTransactionFeatures:
    """Test transaction features database operations"""
    
    @pytest.mark.asyncio
    async def test_get_transaction_features_success(self, db_manager):
        """Test successful transaction features retrieval"""
        transaction_row = {
            'user_id': 'txn_test_user',
            'total_transactions_30d': 15,
            'total_amount_30d': 4500.0,
            'avg_transaction_amount': 300.0,
            'max_transaction_amount': 800.0,
            'transactions_declined_30d': 2,
            'unique_merchants_30d': 10,
            'weekend_transaction_ratio': 0.4,
            'night_transaction_ratio': 0.2,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }
        
        mock_pool, mock_conn = mock_asyncpg_pool()
        mock_conn.fetchrow = AsyncMock(return_value=transaction_row)
        db_manager.pool = mock_pool
        
        result = await db_manager.get_transaction_features('txn_test_user')
        
        assert result is not None
        assert isinstance(result, TransactionFeatures)
        assert result.user_id == 'txn_test_user'
        assert result.total_transactions_30d == 15
        assert result.weekend_transaction_ratio == 0.4
        
        # Verify correct query
        mock_conn.fetchrow.assert_called_once()
        call_args = mock_conn.fetchrow.call_args[0]
        assert 'transaction_features' in call_args[0]
    
    @pytest.mark.asyncio
    async def test_get_transaction_features_with_decimals(self, db_manager):
        """Test transaction features with decimal values"""
        from decimal import Decimal
        
        transaction_row = {
            'user_id': 'decimal_test_user',
            'total_transactions_30d': 8,
            'total_amount_30d': Decimal('2459.75'),
            'avg_transaction_amount': Decimal('307.47'),
            'max_transaction_amount': Decimal('899.99'),
            'transactions_declined_30d': 1,
            'unique_merchants_30d': 6,
            'weekend_transaction_ratio': Decimal('0.375'),
            'night_transaction_ratio': Decimal('0.125'),
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }
        
        mock_pool, mock_conn = mock_asyncpg_pool()
        mock_conn.fetchrow = AsyncMock(return_value=transaction_row)
        db_manager.pool = mock_pool
        
        result = await db_manager.get_transaction_features('decimal_test_user')
        
        assert result is not None
        assert result.total_amount_30d == 2459.75
        assert result.avg_transaction_amount == 307.47
        assert result.weekend_transaction_ratio == 0.375


class TestRiskFeatures:
    """Test risk features database operations"""
    
    @pytest.mark.asyncio
    async def test_get_risk_features_success(self, db_manager):
        """Test successful risk features retrieval"""
        risk_row = {
            'user_id': 'risk_test_user',
            'credit_utilization_ratio': 0.55,
            'payment_delays_30d': 3,
            'payment_delays_90d': 5,
            'failed_payments_count': 2,
            'device_changes_30d': 4,
            'login_locations_30d': 6,
            'velocity_alerts_30d': 1,
            'risk_score': 0.72,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }
        
        mock_pool, mock_conn = mock_asyncpg_pool()
        mock_conn.fetchrow = AsyncMock(return_value=risk_row)
        db_manager.pool = mock_pool
        
        result = await db_manager.get_risk_features('risk_test_user')
        
        assert result is not None
        assert isinstance(result, RiskFeatures)
        assert result.user_id == 'risk_test_user'
        assert result.risk_score == 0.72
        assert result.payment_delays_30d == 3
        assert result.velocity_alerts_30d == 1
        
        # Verify query
        mock_conn.fetchrow.assert_called_once()
        call_args = mock_conn.fetchrow.call_args[0]
        assert 'risk_features' in call_args[0]


class TestBatchFeatures:
    """Test batch feature retrieval operations"""
    
    @pytest.mark.asyncio
    async def test_get_batch_features_success(self, db_manager):
        """Test successful batch features retrieval"""
        user_ids = ['batch_user_1', 'batch_user_2', 'batch_user_3']
        feature_types = ['user', 'transaction']
        
        with patch.object(db_manager, '_get_batch_features_chunk') as mock_chunk:
            mock_chunk.return_value = {
                'batch_user_1': {
                    'user': {'user_id': 'batch_user_1', 'age': 25},
                    'transaction': {'user_id': 'batch_user_1', 'total_transactions_30d': 5}
                },
                'batch_user_2': {
                    'user': {'user_id': 'batch_user_2', 'age': 30}
                }
            }
            
            result = await db_manager.get_batch_features(user_ids, feature_types)
        
        assert len(result) == 3  # Should have entries for all requested users
        assert 'batch_user_1' in result
        assert 'batch_user_2' in result
        assert 'batch_user_3' in result  # Even if empty
        
        # batch_user_1 should have both features
        assert 'user' in result['batch_user_1']
        assert 'transaction' in result['batch_user_1']
        
        # batch_user_2 should have only user features
        assert 'user' in result['batch_user_2']
        assert result['batch_user_2'].get('transaction') is None
    
    @pytest.mark.asyncio
    async def test_get_batch_features_large_batch(self, db_manager):
        """Test batch features with large number of users"""
        # Create 150 user IDs (should be split into 3 batches of 50)
        user_ids = [f'large_batch_user_{i}' for i in range(150)]
        feature_types = ['user']
        
        with patch.object(db_manager, '_get_batch_features_chunk') as mock_chunk:
            # Mock chunk should be called 3 times (150 / 50 = 3)
            mock_chunk.return_value = {}
            
            result = await db_manager.get_batch_features(user_ids, feature_types)
        
        assert mock_chunk.call_count == 3
        assert len(result) == 150
        
        # Verify batch sizes
        call_args_list = mock_chunk.call_args_list
        assert len(call_args_list[0][0][0]) == 50  # First batch: 50 users
        assert len(call_args_list[1][0][0]) == 50  # Second batch: 50 users
        assert len(call_args_list[2][0][0]) == 50  # Third batch: 50 users
    
    @pytest.mark.asyncio
    async def test_batch_query_user_features(self, db_manager):
        """Test batch user features query"""
        mock_conn = MagicMock()
        user_ids = ['user1', 'user2']
        
        mock_rows = [
            {
                'user_id': 'user1',
                'age': 25,
                'total_orders': 10,
                'avg_order_value': 200.0,
                'account_verified': True,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            },
            {
                'user_id': 'user2', 
                'age': 30,
                'total_orders': 15,
                'avg_order_value': 300.0,
                'account_verified': False,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }
        ]
        
        mock_conn.fetch = AsyncMock(return_value=mock_rows)
        
        result = await db_manager._batch_query_user_features(mock_conn, user_ids)
        
        assert len(result) == 2
        assert 'user1' in result
        assert 'user2' in result
        
        # Verify user1 data
        user1_data = result['user1']['user']
        assert user1_data['age'] == 25
        assert user1_data['total_orders'] == 10
        assert user1_data['account_verified']
        
        # Verify query was called with correct parameters
        mock_conn.fetch.assert_called_once()
        call_args = mock_conn.fetch.call_args[0]
        assert 'user_features' in call_args[0]
        assert 'ANY($1)' in call_args[0]  # PostgreSQL array parameter
        assert call_args[1] == user_ids


class TestDatabaseStatistics:
    """Test database statistics and monitoring"""
    
    @pytest.mark.asyncio
    async def test_get_database_stats_success(self, db_manager):
        """Test successful database statistics retrieval"""
        mock_pool, mock_conn = mock_asyncpg_pool()
        
        stats_row = {
            'user_features_count': 50000,
            'transaction_features_count': 45000,
            'risk_features_count': 42000,
            'database_size': 1073741824  # 1GB in bytes
        }
        
        mock_conn.fetchrow = AsyncMock(return_value=stats_row)
        mock_pool.get_size.return_value = 10
        mock_pool.get_max_size.return_value = 20
        
        db_manager.pool = mock_pool
        
        result = await db_manager.get_database_stats()
        
        assert result['user_features_count'] == 50000
        assert result['transaction_features_count'] == 45000
        assert result['risk_features_count'] == 42000
        assert result['database_size_bytes'] == 1073741824
        assert result['pool_size'] == 10
        assert result['pool_max_size'] == 20
        
        # Verify statistics query
        mock_conn.fetchrow.assert_called_once()
        call_args = mock_conn.fetchrow.call_args[0]
        assert 'user_features' in call_args[0]
        assert 'pg_database_size' in call_args[0]
    
    @pytest.mark.asyncio
    async def test_get_database_stats_failure(self, db_manager):
        """Test database statistics retrieval failure"""
        mock_pool, mock_conn = mock_asyncpg_pool()
        mock_conn.fetchrow = AsyncMock(side_effect=Exception("Stats query failed"))
        db_manager.pool = mock_pool
        
        result = await db_manager.get_database_stats()
        
        assert result == {}
    
    @pytest.mark.asyncio
    async def test_get_database_stats_no_pool(self, db_manager):
        """Test database statistics with no pool"""
        db_manager.pool = None
        
        result = await db_manager.get_database_stats()
        
        assert result == {}


class TestDatabaseQueryExecution:
    """Test database query execution utilities"""
    
    @pytest.mark.asyncio
    async def test_execute_select_query_success(self, db_manager, sample_user_row):
        """Test successful SELECT query execution"""
        mock_pool, mock_conn = mock_asyncpg_pool()
        mock_conn.fetchrow = AsyncMock(return_value=sample_user_row)
        db_manager.pool = mock_pool
        
        query = "SELECT * FROM user_features WHERE user_id = $1"
        params = ['test_user']
        
        result = await db_manager._execute_select_query(
            query, params, UserFeatures, 'user_features', 'get_user'
        )
        
        assert result is not None
        assert isinstance(result, UserFeatures)
        assert result.user_id == 'db_test_user_123'
        
        mock_conn.fetchrow.assert_called_once_with(query, 'test_user')
    
    @pytest.mark.asyncio
    async def test_execute_select_query_not_found(self, db_manager):
        """Test SELECT query with no results"""
        mock_pool, mock_conn = mock_asyncpg_pool()
        mock_conn.fetchrow = AsyncMock(return_value=None)
        db_manager.pool = mock_pool
        
        query = "SELECT * FROM user_features WHERE user_id = $1"
        params = ['nonexistent_user']
        
        result = await db_manager._execute_select_query(
            query, params, UserFeatures, 'user_features', 'get_user'
        )
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_execute_update_query_success(self, db_manager):
        """Test successful UPDATE query execution"""
        mock_pool, mock_conn = mock_asyncpg_pool()
        mock_conn.execute = AsyncMock()
        db_manager.pool = mock_pool
        
        query = "UPDATE user_features SET total_orders = $2 WHERE user_id = $1"
        params = ['test_user', 20]
        
        result = await db_manager._execute_update_query(
            query, params, 'user_features', 'update_user'
        )
        
        assert result
        mock_conn.execute.assert_called_once_with(query, 'test_user', 20)
    
    @pytest.mark.asyncio
    async def test_execute_update_query_failure(self, db_manager):
        """Test UPDATE query execution failure"""
        mock_pool, mock_conn = mock_asyncpg_pool()
        mock_conn.execute = AsyncMock(side_effect=Exception("Update failed"))
        db_manager.pool = mock_pool
        
        query = "UPDATE user_features SET total_orders = $2 WHERE user_id = $1"
        params = ['test_user', 20]
        
        result = await db_manager._execute_update_query(
            query, params, 'user_features', 'update_user'
        )
        
        assert not result
    
    @pytest.mark.asyncio
    async def test_execute_query_no_pool(self, db_manager):
        """Test query execution with no connection pool"""
        db_manager.pool = None
        
        result = await db_manager._execute_select_query(
            "SELECT 1", [], UserFeatures, 'test', 'test'
        )
        
        assert result is None


class TestDatabaseConnectionManagement:
    """Test database connection management and error handling"""
    
    @pytest.mark.asyncio
    async def test_connection_acquisition_failure(self, db_manager):
        """Test handling of connection acquisition failure"""
        mock_pool = MagicMock()
        mock_pool.acquire.side_effect = Exception("Connection pool exhausted")
        db_manager.pool = mock_pool
        
        result = await db_manager.get_user_features('test_user')
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_connection_timeout_handling(self, db_manager):
        """Test handling of connection timeouts"""
        mock_pool, mock_conn = mock_asyncpg_pool()
        mock_conn.fetchrow = AsyncMock(side_effect=asyncio.TimeoutError("Query timeout"))
        db_manager.pool = mock_pool
        
        result = await db_manager.get_user_features('test_user')
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_multiple_concurrent_connections(self, db_manager, sample_user_row):
        """Test handling of multiple concurrent database connections"""
        mock_pool, mock_conn = mock_asyncpg_pool()
        mock_conn.fetchrow = AsyncMock(return_value=sample_user_row)
        db_manager.pool = mock_pool
        
        # Execute multiple concurrent queries
        tasks = [
            db_manager.get_user_features(f'concurrent_user_{i}')
            for i in range(10)
        ]
        
        results = await asyncio.gather(*tasks)
        
        # All queries should succeed
        assert len(results) == 10
        for result in results:
            assert result is not None
            assert isinstance(result, UserFeatures)
        
        # Connection should be acquired 10 times
        assert mock_pool.acquire.call_count == 10