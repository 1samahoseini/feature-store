"""
Unit tests for data pipelines
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, timedelta

from src.pipelines.batch_pipeline import BatchPipeline
from src.pipelines.feature_pipeline import FeaturePipeline
from src.pipelines.data_quality import DataQualityPipeline
from src.pipelines.migration import DatabaseMigration


class TestBatchPipeline:
    """Test batch feature computation pipeline"""
    
    @pytest.fixture
    def batch_pipeline(self):
        """Create batch pipeline instance for testing"""
        return BatchPipeline()
    
    @pytest.fixture
    def mock_db_pool(self):
        """Mock database pool for testing"""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        return mock_pool, mock_conn
    
    @pytest.mark.asyncio
    async def test_run_daily_pipeline_success(self, batch_pipeline, mock_db_pool):
        """Test successful daily pipeline execution"""
        mock_pool, mock_conn = mock_db_pool
        
        # Mock database responses
        mock_conn.fetch.return_value = [
            MagicMock(user_id="user1", age=25, total_orders=5),
            MagicMock(user_id="user2", age=30, total_orders=10),
        ]
        mock_conn.executemany = AsyncMock()
        
        with patch('src.pipelines.batch_pipeline.db') as mock_db:
            mock_db.pool = mock_pool
            
            # Mock BigQuery client
            with patch.object(batch_pipeline, 'bigquery_client') as mock_bq:
                mock_bq.dataset.return_value.table.return_value = MagicMock()
                
                results = await batch_pipeline.run_daily_pipeline()
        
        # Verify results
        assert results['status'] == 'completed'
        assert 'processed_records' in results
        assert 'duration_seconds' in results
        assert results['total_records'] > 0
        
        # Verify database interactions
        assert mock_conn.fetch.called
        assert mock_conn.executemany.called
    
    @pytest.mark.asyncio
    async def test_compute_user_features(self, batch_pipeline, mock_db_pool):
        """Test user features computation"""
        mock_pool, mock_conn = mock_db_pool
        
        # Mock user data
        mock_users = [
            {
                'user_id': 'user1',
                'age': 28,
                'location_country': 'AE',
                'location_city': 'Dubai',
                'total_orders': 15,
                'avg_order_value': 250.0,
                'days_since_first_order': 60,
                'preferred_payment_method': 'bnpl',
                'account_verified': True
            }
        ]
        
        mock_conn.fetch.return_value = [MagicMock(**user) for user in mock_users]
        mock_conn.executemany = AsyncMock()
        
        with patch('src.pipelines.batch_pipeline.db') as mock_db:
            mock_db.pool = mock_pool
            
            result = await batch_pipeline._compute_user_features()
        
        assert result['count'] == 1
        assert 'duration' in result
        
        # Verify the upsert query was called
        mock_conn.executemany.assert_called_once()
        call_args = mock_conn.executemany.call_args
        query = call_args[0][0]
        assert 'INSERT INTO user_features' in query
        assert 'ON CONFLICT (user_id) DO UPDATE' in query
    
    @pytest.mark.asyncio
    async def test_compute_transaction_features(self, batch_pipeline, mock_db_pool):
        """Test transaction features computation"""
        mock_pool, mock_conn = mock_db_pool
        
        # Mock transaction data
        mock_transactions = [
            {
                'user_id': 'user1',
                'total_transactions_30d': 8,
                'total_amount_30d': 2400.0,
                'avg_transaction_amount': 300.0,
                'max_transaction_amount': 800.0,
                'transactions_declined_30d': 1,
                'unique_merchants_30d': 6,
                'weekend_transaction_ratio': 0.25,
                'night_transaction_ratio': 0.125
            }
        ]
        
        mock_conn.fetch.return_value = [MagicMock(**txn) for txn in mock_transactions]
        mock_conn.executemany = AsyncMock()
        
        with patch('src.pipelines.batch_pipeline.db') as mock_db:
            mock_db.pool = mock_pool
            
            result = await batch_pipeline._compute_transaction_features()
        assert result['count'] == 1
        assert 'duration' in result
        
        # Verify the query includes transaction-specific calculations
        mock_conn.executemany.assert_called_once()
        call_args = mock_conn.executemany.call_args
        query = call_args[0][0]
        assert 'INSERT INTO transaction_features' in query
        assert 'total_transactions_30d' in query
        assert 'weekend_transaction_ratio' in query
    
    @pytest.mark.asyncio
    async def test_compute_risk_features(self, batch_pipeline, mock_db_pool):
        """Test risk features computation"""
        mock_pool, mock_conn = mock_db_pool
        
        # Mock risk data
        mock_risk_data = [
            {
                'user_id': 'user1',
                'credit_utilization_ratio': 0.35,
                'payment_delays_30d': 2,
                'payment_delays_90d': 3,
                'failed_payments_count': 1,
                'device_changes_30d': 2,
                'login_locations_30d': 3,
                'velocity_alerts_30d': 0,
                'risk_score': 0.45
            }
        ]
        
        mock_conn.fetch.return_value = [MagicMock(**risk) for risk in mock_risk_data]
        mock_conn.executemany = AsyncMock()
        
        with patch('src.pipelines.batch_pipeline.db') as mock_db:
            mock_db.pool = mock_pool
            
            result = await batch_pipeline._compute_risk_features()
        
        assert result['count'] == 1
        assert 'duration' in result
        
        # Verify risk-specific calculations
        mock_conn.executemany.assert_called_once()
        call_args = mock_conn.executemany.call_args
        query = call_args[0][0]
        assert 'INSERT INTO risk_features' in query
        assert 'credit_utilization_ratio' in query
        assert 'risk_score' in query
    
    @pytest.mark.asyncio
    async def test_pipeline_failure_handling(self, batch_pipeline):
        """Test pipeline failure handling"""
        with patch('src.pipelines.batch_pipeline.db') as mock_db:
            mock_db.pool = None  # Simulate database unavailable
            
            result = await batch_pipeline.run_daily_pipeline()
        
        assert result['status'] == 'failed'
        assert 'error' in result
        assert 'duration_seconds' in result
    
    @pytest.mark.asyncio
    async def test_bigquery_export(self, batch_pipeline, mock_db_pool):
        """Test BigQuery data export"""
        mock_pool, mock_conn = mock_db_pool
        
        # Mock data for export
        mock_data = [
            MagicMock(user_id='user1', age=28, total_orders=10, updated_at=datetime.utcnow()),
            MagicMock(user_id='user2', age=32, total_orders=15, updated_at=datetime.utcnow())
        ]
        mock_conn.fetch.return_value = mock_data
        
        # Mock BigQuery client
        mock_bq_client = MagicMock()
        mock_table_ref = MagicMock()
        mock_job = MagicMock()
        
        mock_bq_client.dataset.return_value.table.return_value = mock_table_ref
        mock_bq_client.load_table_from_json.return_value = mock_job
        
        batch_pipeline.bigquery_client = mock_bq_client
        
        with patch('src.pipelines.batch_pipeline.db') as mock_db:
            mock_db.pool = mock_pool
            
            await batch_pipeline._export_to_bigquery('user_features', 'test_dataset')
        
        # Verify BigQuery operations
        mock_bq_client.dataset.assert_called_with('test_dataset')
        mock_bq_client.load_table_from_json.assert_called_once()
        mock_job.result.assert_called_once()  # Wait for job completion


class TestFeaturePipeline:
    """Test streaming feature pipeline"""
    
    @pytest.fixture
    def feature_pipeline(self):
        """Create feature pipeline instance for testing"""
        return FeaturePipeline()
    
    @pytest.fixture
    def mock_kafka_consumer(self):
        """Mock Kafka consumer"""
        with patch('src.pipelines.feature_pipeline.KafkaConsumer') as mock:
            consumer_instance = MagicMock()
            mock.return_value = consumer_instance
            yield consumer_instance
    
    @pytest.fixture
    def mock_kafka_producer(self):
        """Mock Kafka producer"""
        with patch('src.pipelines.feature_pipeline.KafkaProducer') as mock:
            producer_instance = MagicMock()
            mock.return_value = producer_instance
            yield producer_instance
    
    @pytest.mark.asyncio
    async def test_start_pipeline(self, feature_pipeline, mock_kafka_consumer, mock_kafka_producer):
        """Test pipeline startup"""
        # Mock successful startup
        with patch.object(feature_pipeline, '_process_messages') as mock_process:
            mock_process.return_value = asyncio.Future()
            mock_process.return_value.set_result(None)
            
            await feature_pipeline.start()
        
        assert feature_pipeline.running
        assert feature_pipeline.consumer is not None
        assert feature_pipeline.producer is not None
    
    @pytest.mark.asyncio
    async def test_stop_pipeline(self, feature_pipeline):
        """Test pipeline shutdown"""
        # Setup running pipeline
        feature_pipeline.running = True
        feature_pipeline.consumer = MagicMock()
        feature_pipeline.producer = MagicMock()
        
        await feature_pipeline.stop()
        
        assert not feature_pipeline.running
        feature_pipeline.consumer.close.assert_called_once()
        feature_pipeline.producer.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_process_user_update_event(self, feature_pipeline):
        """Test processing user update events"""
        with patch('src.pipelines.feature_pipeline.cache') as mock_cache:
            mock_cache.delete_user_features = AsyncMock(return_value=True)
            
            event_data = {
                'event_type': 'user_updated',
                'user_id': 'test_user_123',
                'recompute_features': False
            }
            
            result = await feature_pipeline._process_feature_event('user_updated', event_data)
        
        assert result
        mock_cache.delete_user_features.assert_called_once_with('test_user_123')
    
    @pytest.mark.asyncio
    async def test_process_transaction_event(self, feature_pipeline):
        """Test processing transaction completion events"""
        with patch('src.pipelines.feature_pipeline.cache') as mock_cache:
            mock_cache.delete_user_features = AsyncMock(return_value=True)
            
            with patch.object(feature_pipeline, '_update_realtime_counters') as mock_counters:
                mock_counters.return_value = asyncio.Future()
                mock_counters.return_value.set_result(None)
                
                event_data = {
                    'event_type': 'transaction_completed',
                    'user_id': 'test_user_123',
                    'amount': 299.99,
                    'merchant_id': 'merchant_abc'
                }
                
                result = await feature_pipeline._process_feature_event('transaction_completed', event_data)
        
        assert result
        mock_cache.delete_user_features.assert_called_once_with('test_user_123')
        mock_counters.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_process_risk_update_event(self, feature_pipeline):
        """Test processing risk score update events"""
        with patch('src.pipelines.feature_pipeline.cache') as mock_cache:
            mock_cache.delete_user_features = AsyncMock(return_value=True)
            
            with patch.object(feature_pipeline, '_update_risk_score') as mock_risk_update:
                mock_risk_update.return_value = asyncio.Future()
                mock_risk_update.return_value.set_result(None)
                
                event_data = {
                    'event_type': 'risk_score_updated',
                    'user_id': 'test_user_123',
                    'risk_score': 0.65
                }
                
                result = await feature_pipeline._process_feature_event('risk_score_updated', event_data)
        
        assert result
        mock_cache.delete_user_features.assert_called_once_with('test_user_123')
        mock_risk_update.assert_called_once_with('test_user_123', 0.65)
    
    @pytest.mark.asyncio
    async def test_publish_feature_update(self, feature_pipeline):
        """Test publishing feature update events"""
        mock_producer = MagicMock()
        feature_pipeline.producer = mock_producer
        feature_pipeline.settings = MagicMock()
        feature_pipeline.settings.kafka_topic_features = 'test-topic'
        
        await feature_pipeline.publish_feature_update(
            'user_123', 
            'user_features', 
            {'total_orders': 20, 'avg_order_value': 150.0}
        )
        
        mock_producer.send.assert_called_once()
        call_args = mock_producer.send.call_args
        
        assert call_args[0][0] == 'test-topic'  # Topic
        event = call_args[1]['value']  # Event data
        assert event['user_id'] == 'user_123'
        assert event['feature_type'] == 'user_features'
        assert event['data']['total_orders'] == 20
    
    @pytest.mark.asyncio
    async def test_unknown_event_type(self, feature_pipeline):
        """Test handling of unknown event types"""
        event_data = {
            'event_type': 'unknown_event',
            'user_id': 'test_user_123'
        }
        
        result = await feature_pipeline._process_feature_event('unknown_event', event_data)
        
        assert not result


class TestDataQualityPipeline:
    """Test data quality monitoring pipeline"""
    
    @pytest.fixture
    def quality_pipeline(self):
        """Create data quality pipeline instance for testing"""
        return DataQualityPipeline()
    
    @pytest.fixture
    def mock_db_connection(self):
        """Mock database connection for testing"""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        return mock_pool, mock_conn
    
    @pytest.mark.asyncio
    async def test_run_quality_checks_success(self, quality_pipeline, mock_db_connection):
        """Test successful quality checks execution"""
        mock_pool, mock_conn = mock_db_connection
        
        with patch('src.pipelines.data_quality.db') as mock_db:
            mock_db.pool = mock_pool
            
            with patch('src.pipelines.data_quality.cache') as mock_cache:
                mock_cache.health_check = AsyncMock(return_value=True)
                mock_cache.get_cache_stats = AsyncMock(return_value={'keyspace_hits': 1000, 'keyspace_misses': 200})
                
                # Mock database responses for quality checks
                mock_conn.fetchrow.return_value = MagicMock(
                    latest_update=datetime.utcnow() - timedelta(minutes=30),
                    total_records=1000,
                    fresh_records=950
                )
                mock_conn.fetchval.return_value = 1000
                
                results = await quality_pipeline.run_quality_checks()
        
        assert results['status'] == 'completed'
        assert 'checks' in results
        assert 'alerts' in results
        assert 'summary' in results
        
        # Verify all check types were performed
        assert 'freshness' in results['checks']
        assert 'completeness' in results['checks']
        assert 'anomalies' in results['checks']
        assert 'cache' in results['checks']
    
    @pytest.mark.asyncio
    async def test_check_data_freshness(self, quality_pipeline, mock_db_connection):
        """Test data freshness checking"""
        mock_pool, mock_conn = mock_db_connection
        
        # Mock fresh data
        fresh_update_time = datetime.utcnow() - timedelta(minutes=30)
        mock_conn.fetchrow.return_value = MagicMock(
            latest_update=fresh_update_time,
            total_records=1000,
            fresh_records=900
        )
        
        with patch('src.pipelines.data_quality.db') as mock_db:
            mock_db.pool = mock_pool
            
            results = await quality_pipeline._check_data_freshness()
        
        # Should have results for all tables
        assert 'user_features' in results
        assert 'transaction_features' in results
        assert 'risk_features' in results
        
        # Check freshness calculation
        for table_result in results.values():
            if 'error' not in table_result:
                assert table_result['is_fresh']
                assert table_result['freshness_minutes'] <= 120  # 2 hours threshold
                assert table_result['fresh_ratio'] >= 0.8
    
    @pytest.mark.asyncio
    async def test_check_data_completeness(self, quality_pipeline, mock_db_connection):
        """Test data completeness checking"""
        mock_pool, mock_conn = mock_db_connection
        
        # Mock completeness data
        mock_conn.fetchval.side_effect = [
            1000,  # total_count for table
            950,   # non_null_count for first field
            980,   # non_null_count for second field
            990    # non_null_count for third field
        ]
        
        with patch('src.pipelines.data_quality.db') as mock_db:
            mock_db.pool = mock_pool
            
            results = await quality_pipeline._check_data_completeness()
        
        # Should have completeness results
        assert 'user_features' in results
        
        # Check completeness calculations
        user_features = results['user_features']
        for field_result in user_features.values():
            assert 'completeness_ratio' in field_result
            assert field_result['completeness_ratio'] >= 0.95  # Should meet threshold
            assert field_result['is_complete'] == True  # noqa: E712
    
    @pytest.mark.asyncio
    async def test_check_data_anomalies(self, quality_pipeline, mock_db_connection):
        """Test data anomaly detection"""
        mock_pool, mock_conn = mock_db_connection
        
        # Mock statistical data
        mock_conn.fetchrow.return_value = MagicMock(
            mean=100.0,
            stddev=20.0,
            min_val=10.0,
            max_val=500.0,
            total_count=1000,
            outlier_count=25  # 2.5% outliers (acceptable)
        )
        
        with patch('src.pipelines.data_quality.db') as mock_db:
            mock_db.pool = mock_pool
            
            results = await quality_pipeline._check_data_anomalies()
        
        # Should have anomaly results for all tables
        assert 'user_features' in results
        assert 'transaction_features' in results
        assert 'risk_features' in results
        
        # Check anomaly detection
        for table_results in results.values():
            if isinstance(table_results, dict) and 'error' not in table_results:
                for field_result in table_results.values():
                    if isinstance(field_result, dict):
                        assert 'outlier_ratio' in field_result
                        assert not field_result['has_anomalies']  # Low outlier rate
    
    @pytest.mark.asyncio
    async def test_generate_quality_alerts(self, quality_pipeline):
        """Test quality alert generation"""
        # Mock check results with some failures
        check_results = {
            'freshness': {
                'user_features': {'is_fresh': False, 'freshness_minutes': 180},
                'transaction_features': {'is_fresh': True, 'freshness_minutes': 45}
            },
            'completeness': {
                'user_features': {
                    'age': {'is_complete': False, 'completeness_ratio': 0.85},
                    'email': {'is_complete': True, 'completeness_ratio': 0.99}
                }
            },
            'anomalies': {
                'risk_features': {
                    'risk_score': {'has_anomalies': True, 'outlier_count': 100, 'outlier_ratio': 0.1}
                }
            },
            'cache': {'is_healthy': False}
        }
        
        alerts = quality_pipeline._generate_quality_alerts(check_results)
        
        # Should generate alerts for each issue
        assert len(alerts) == 4  # Freshness, completeness, anomaly, cache
        
        # Check alert types
        alert_types = [alert['type'] for alert in alerts]
        assert 'data_freshness' in alert_types
        assert 'data_completeness' in alert_types
        assert 'data_anomaly' in alert_types
        assert 'cache_health' in alert_types
        
        # Check alert severities
        severities = [alert['severity'] for alert in alerts]
        assert 'error' in severities  # Completeness and cache issues
        assert 'warning' in severities  # Freshness and anomaly issues
    
    @pytest.mark.asyncio
    async def test_validate_user_features(self, quality_pipeline):
        """Test user features validation"""
        # Valid user features
        valid_features = {
            'age': 28,
            'total_orders': 15,
            'avg_order_value': 250.0
        }
        
        validation = quality_pipeline._validate_user_features(valid_features)
        
        assert len(validation['errors']) == 0
        assert len(validation['warnings']) == 0
        
        # Invalid user features
        invalid_features = {
            'age': 15,  # Too young
            'total_orders': -5,  # Negative
            'avg_order_value': -100.0  # Negative
        }
        
        validation = quality_pipeline._validate_user_features(invalid_features)
        
        assert len(validation['errors']) == 2  # Negative values
        assert len(validation['warnings']) == 1  # Age warning
    
    @pytest.mark.asyncio
    async def test_validate_risk_features(self, quality_pipeline):
        """Test risk features validation"""
        # Valid risk features
        valid_features = {
            'risk_score': 0.45,
            'credit_utilization_ratio': 0.35,
            'payment_delays_30d': 2,
            'payment_delays_90d': 3
        }
        
        validation = quality_pipeline._validate_risk_features(valid_features)
        
        assert len(validation['errors']) == 0
        assert len(validation['warnings']) == 0
        
        # Invalid risk features
        invalid_features = {
            'risk_score': 1.5,  # Out of range
            'credit_utilization_ratio': -0.1,  # Out of range
            'payment_delays_30d': 5,
            'payment_delays_90d': 3  # 30d > 90d (unusual)
        }
        
        validation = quality_pipeline._validate_risk_features(invalid_features)
        
        assert len(validation['errors']) == 2  # Out of range values
        assert len(validation['warnings']) == 1  # Unusual pattern


class TestDatabaseMigration:
    """Test database migration utilities"""
    
    @pytest.fixture
    def migration(self):
        """Create migration instance for testing"""
        return DatabaseMigration()
    
    @pytest.fixture
    def mock_connections(self):
        """Mock database connections"""
        mock_pg_pool = MagicMock()
        mock_crdb_pool = MagicMock()
        mock_pg_conn = MagicMock()
        mock_crdb_conn = MagicMock()
        
        mock_pg_pool.acquire.return_value.__aenter__.return_value = mock_pg_conn
        mock_crdb_pool.acquire.return_value.__aenter__.return_value = mock_crdb_conn
        
        return {
            'pg_pool': mock_pg_pool,
            'crdb_pool': mock_crdb_pool,
            'pg_conn': mock_pg_conn,
            'crdb_conn': mock_crdb_conn
        }
    
    @pytest.mark.asyncio
    async def test_validate_migration_prerequisites(self, migration, mock_connections):
        """Test migration prerequisites validation"""
        migration.pg_pool = mock_connections['pg_pool']
        migration.crdb_pool = mock_connections['crdb_pool']
        
        # Mock successful validations
        with patch.object(migration, '_validate_postgresql') as mock_pg_val:
            mock_pg_val.return_value = asyncio.Future()
            mock_pg_val.return_value.set_result({'status': 'pass', 'total_records': 10000})
            
            with patch.object(migration, '_validate_cockroachdb') as mock_crdb_val:
                mock_crdb_val.return_value = asyncio.Future()
                mock_crdb_val.return_value.set_result({'status': 'pass', 'ready_for_migration': True})
                
                with patch.object(migration, '_validate_data_consistency') as mock_consistency:
                    mock_consistency.return_value = asyncio.Future()
                    mock_consistency.return_value.set_result({'status': 'pass', 'issues': []})
                    
                    with patch.object(migration, '_validate_resources') as mock_resources:
                        mock_resources.return_value = asyncio.Future()
                        mock_resources.return_value.set_result({'status': 'pass'})
                        
                        results = await migration.validate_migration_prerequisites()
        
        assert results['ready_for_migration']
        assert results['status'] == 'completed'
        assert 'checks' in results
        
        # All validation methods should have been called
        mock_pg_val.assert_called_once()
        mock_crdb_val.assert_called_once()
        mock_consistency.assert_called_once()
        mock_resources.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_validate_postgresql_success(self, migration, mock_connections):
        """Test PostgreSQL validation"""
        migration.pg_pool = mock_connections['pg_pool']
        mock_conn = mock_connections['pg_conn']
        
        # Mock database responses
        mock_conn.fetchval.side_effect = [
            None,  # SELECT 1 (connection test)
            1000,  # user_features count
            800,   # transaction_features count
            900    # risk_features count
        ]
        
        latest_update = datetime.utcnow() - timedelta(hours=1)
        mock_conn.fetchval.side_effect.extend([latest_update, latest_update, latest_update])
        
        result = await migration._validate_postgresql()
        
        assert result['status'] == 'pass'
        assert result['connection'] == 'healthy'
        assert result['total_records'] == 2700  # Sum of all tables
        assert 'table_counts' in result
        assert 'latest_updates' in result
    
    @pytest.mark.asyncio
    async def test_validate_cockroachdb_ready(self, migration, mock_connections):
        """Test CockroachDB validation when ready for migration"""
        migration.crdb_pool = mock_connections['crdb_pool']
        mock_conn = mock_connections['crdb_conn']
        
        # Mock successful connection and version check
        mock_conn.fetchval.side_effect = [
            None,  # SELECT 1 (connection test)
            "CockroachDB CCL v23.1.0"  # version()
        ]
        
        # Mock that tables don't exist yet (good for migration)
        async def mock_fetchval_tables(*args):
            raise Exception("Table doesn't exist")
        
        mock_conn.fetchval.side_effect.append(mock_fetchval_tables)
        
        result = await migration._validate_cockroachdb()
        
        assert result['status'] == 'pass'
        assert result['connection'] == 'healthy'
        assert result['ready_for_migration']
        assert len(result['existing_tables']) == 0
    
    @pytest.mark.asyncio
    async def test_migration_execution_success(self, migration, mock_connections):
        """Test successful migration execution"""
        migration.pg_pool = mock_connections['pg_pool']
        migration.crdb_pool = mock_connections['crdb_pool']
        
        # Mock migration phases
        with patch.object(migration, '_migrate_schema') as mock_schema:
            mock_schema.return_value = asyncio.Future()
            mock_schema.return_value.set_result({'status': 'success'})
            
            with patch.object(migration, '_migrate_data') as mock_data:
                mock_data.return_value = asyncio.Future()
                mock_data.return_value.set_result({'status': 'success', 'total_records': 5000})
                
                with patch.object(migration, '_validate_migration') as mock_validate:
                    mock_validate.return_value = asyncio.Future()
                    mock_validate.return_value.set_result({'status': 'success'})
                    
                    migration_config = {'batch_size': 1000, 'auto_switchover': False}
                    results = await migration.run_migration(migration_config)
        
        assert results['status'] == 'completed'
        assert 'phases' in results
        assert results['phases']['schema']['status'] == 'success'
        assert results['phases']['data']['status'] == 'success'
        assert results['phases']['validation']['status'] == 'success'
        
        # All phase methods should have been called
        mock_schema.assert_called_once()
        mock_data.assert_called_once_with(migration_config)
        mock_validate.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_migration_failure_handling(self, migration, mock_connections):
        """Test migration failure handling"""
        migration.pg_pool = mock_connections['pg_pool']
        migration.crdb_pool = mock_connections['crdb_pool']
        
        # Mock schema migration failure
        with patch.object(migration, '_migrate_schema') as mock_schema:
            mock_schema.return_value = asyncio.Future()
            mock_schema.return_value.set_result({'status': 'error', 'error': 'Schema creation failed'})
            
            migration_config = {'batch_size': 1000}
            results = await migration.run_migration(migration_config)
        
        assert results['status'] == 'failed'
        assert 'Schema migration failed' in results['error']
    
    @pytest.mark.asyncio
    async def test_migrate_table_batch_processing(self, migration, mock_connections):
        """Test table migration with batch processing"""
        mock_pg_conn = mock_connections['pg_conn']
        mock_crdb_conn = mock_connections['crdb_conn']
        
        # Mock column information
        mock_pg_conn.fetch.return_value = [
            MagicMock(column_name='user_id'),
            MagicMock(column_name='age'),
            MagicMock(column_name='total_orders')
        ]
        
        # Mock total count
        mock_pg_conn.fetchval.return_value = 2500  # Total records
        
        # Mock batched data
        batch_1 = [MagicMock(user_id='user1', age=25, total_orders=5)]
        batch_2 = [MagicMock(user_id='user2', age=30, total_orders=10)]
        
        mock_pg_conn.fetch.side_effect = [
            # First call returns columns, subsequent calls return data batches
            [MagicMock(column_name='user_id'), MagicMock(column_name='age')],
            batch_1,
            batch_2,
            []  # Empty batch to end loop
        ]
        
        mock_crdb_conn.executemany = AsyncMock()
        
        migration.pg_pool = mock_connections['pg_pool']
        migration.crdb_pool = mock_connections['crdb_pool']
        
        result = await migration._migrate_table('user_features', 1000)
        
        assert result == 2  # Should have migrated 2 records total
        
        # Should have called executemany twice (once per batch)
        assert mock_crdb_conn.executemany.call_count == 2