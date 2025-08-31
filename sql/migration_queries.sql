-- Database Migration Queries
-- PostgreSQL to CockroachDB migration utilities

-- Data validation queries
-- Check for data consistency before migration

-- 1. Check for duplicate primary keys
SELECT 'user_features' as table_name, user_id, COUNT(*) as duplicate_count
FROM user_features
GROUP BY user_id
HAVING COUNT(*) > 1

UNION ALL

SELECT 'transaction_features' as table_name, user_id, COUNT(*) as duplicate_count
FROM transaction_features
GROUP BY user_id
HAVING COUNT(*) > 1

UNION ALL

SELECT 'risk_features' as table_name, user_id, COUNT(*) as duplicate_count
FROM risk_features
GROUP BY user_id
HAVING COUNT(*) > 1;

-- 2. Check for referential integrity
SELECT 'orphaned_user_features' as issue, COUNT(*) as count
FROM user_features uf
LEFT JOIN users u ON uf.user_id = u.user_id
WHERE u.user_id IS NULL

UNION ALL

SELECT 'orphaned_transaction_features' as issue, COUNT(*) as count
FROM transaction_features tf
LEFT JOIN users u ON tf.user_id = u.user_id
WHERE u.user_id IS NULL

UNION ALL

SELECT 'orphaned_risk_features' as issue, COUNT(*) as count
FROM risk_features rf
LEFT JOIN users u ON rf.user_id = u.user_id
WHERE u.user_id IS NULL;

-- 3. Data type compatibility check
SELECT 
  'data_type_ranges' as check_name,
  COUNT(*) FILTER (WHERE age < 18 OR age > 100) as invalid_age_count,
  COUNT(*) FILTER (WHERE total_orders < 0) as negative_orders_count,
  COUNT(*) FILTER (WHERE avg_order_value < 0) as negative_amount_count
FROM user_features;

-- Pre-migration data export queries
-- Export data in CockroachDB compatible format

-- Export user_features
SELECT 
  user_id,
  age,
  location_country,
  location_city,
  total_orders,
  avg_order_value,
  days_since_first_order,
  preferred_payment_method,
  account_verified,
  created_at,
  updated_at
FROM user_features
ORDER BY user_id;

-- Export transaction_features
SELECT 
  user_id,
  total_transactions_30d,
  total_amount_30d,
  avg_transaction_amount,
  max_transaction_amount,
  transactions_declined_30d,
  unique_merchants_30d,
  weekend_transaction_ratio,
  night_transaction_ratio,
  created_at,
  updated_at
FROM transaction_features
ORDER BY user_id;

-- Export risk_features
SELECT 
  user_id,
  credit_utilization_ratio,
  payment_delays_30d,
  payment_delays_90d,
  failed_payments_count,
  device_changes_30d,
  login_locations_30d,
  velocity_alerts_30d,
  risk_score,
  created_at,
  updated_at
FROM risk_features
ORDER BY user_id;

-- Post-migration validation queries
-- Compare record counts and data integrity

-- Count comparison
SELECT 'postgresql_counts' as source, 
       (SELECT COUNT(*) FROM user_features) as user_features,
       (SELECT COUNT(*) FROM transaction_features) as transaction_features,
       (SELECT COUNT(*) FROM risk_features) as risk_features;

-- Data sampling for validation
-- Sample 1000 random records for detailed comparison
WITH random_users AS (
  SELECT user_id 
  FROM users 
  ORDER BY RANDOM() 
  LIMIT 1000
)
SELECT 
  'sample_data' as data_type,
  u.user_id,
  uf.total_orders,
  uf.avg_order_value,
  tf.total_transactions_30d,
  tf.total_amount_30d,
  rf.risk_score,
  rf.credit_utilization_ratio
FROM random_users ru
JOIN users u ON ru.user_id = u.user_id
LEFT JOIN user_features uf ON u.user_id = uf.user_id
LEFT JOIN transaction_features tf ON u.user_id = tf.user_id
LEFT JOIN risk_features rf ON u.user_id = rf.user_id
ORDER BY u.user_id;

-- Performance comparison queries
-- Benchmark query performance between databases

-- Feature retrieval performance test
EXPLAIN (ANALYZE, BUFFERS)
SELECT 
  uf.user_id,
  uf.total_orders,
  uf.avg_order_value,
  tf.total_transactions_30d,
  tf.total_amount_30d,
  rf.risk_score
FROM user_features uf
LEFT JOIN transaction_features tf ON uf.user_id = tf.user_id
LEFT JOIN risk_features rf ON uf.user_id = rf.user_id
WHERE uf.user_id = 'test_user_123456';

-- Batch retrieval performance test
EXPLAIN (ANALYZE, BUFFERS)
SELECT 
  uf.user_id,
  uf.total_orders,
  tf.total_transactions_30d,
  rf.risk_score
FROM user_features uf
LEFT JOIN transaction_features tf ON uf.user_id = tf.user_id
LEFT JOIN risk_features rf ON uf.user_id = rf.user_id
WHERE uf.user_id = ANY(ARRAY['user1', 'user2', 'user3', 'user4', 'user5']);

-- Index usage analysis
SELECT 
  schemaname,
  tablename,
  indexname,
  idx_tup_read,
  idx_tup_fetch,
  idx_scan
FROM pg_stat_user_indexes
WHERE tablename IN ('user_features', 'transaction_features', 'risk_features')
ORDER BY idx_scan DESC;

-- Migration rollback queries (if needed)
-- Clean up CockroachDB tables for re-migration

TRUNCATE TABLE risk_features CASCADE;
TRUNCATE TABLE transaction_features CASCADE; 
TRUNCATE TABLE user_features CASCADE;
TRUNCATE TABLE users CASCADE;

-- Reset sequences and identity columns if any
-- (CockroachDB uses different syntax for this)

-- Migration progress monitoring
-- Track migration progress in real-time

CREATE TABLE IF NOT EXISTS migration_progress (
  id SERIAL PRIMARY KEY,
  table_name VARCHAR(100) NOT NULL,
  total_records INTEGER DEFAULT 0,
  migrated_records INTEGER DEFAULT 0,
  start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  end_time TIMESTAMP,
  status VARCHAR(20) DEFAULT 'in_progress',
  error_message TEXT
);

-- Insert initial progress tracking
INSERT INTO migration_progress (table_name, total_records, status) VALUES
('users', (SELECT COUNT(*) FROM users), 'pending'),
('user_features', (SELECT COUNT(*) FROM user_features), 'pending'),
('transaction_features', (SELECT COUNT(*) FROM transaction_features), 'pending'),
('risk_features', (SELECT COUNT(*) FROM risk_features), 'pending');

-- Update progress during migration
UPDATE migration_progress 
SET migrated_records = $1, status = $2, end_time = CURRENT_TIMESTAMP
WHERE table_name = $3;

-- Final migration report
SELECT 
  table_name,
  total_records,
  migrated_records,
  CASE 
    WHEN migrated_records = total_records THEN 'SUCCESS'
    WHEN migrated_records > 0 THEN 'PARTIAL'
    ELSE 'FAILED'
  END as migration_status,
  EXTRACT(EPOCH FROM (end_time - start_time)) as duration_seconds,
  error_message
FROM migration_progress
ORDER BY table_name;