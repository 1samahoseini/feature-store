# BNPL Feature Store - Database Strategy

## Overview

The Feature Store uses a flexible database architecture supporting both PostgreSQL (current) and CockroachDB (future) for transactional data, with BigQuery for analytical workloads. This document covers schema design, migration strategy, and performance optimization.

## Database Architecture

### Current State: PostgreSQL┌─────────────────┐ │   PostgreSQL    │ │                 │ │ • OLTP workloads│ │ • Single node   │ │ • 15GB storage  │ │ • ACID compliant│ └─────────────────┘### Future State: CockroachDB┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐ │  CockroachDB    │    │  CockroachDB    │    │  CockroachDB    │ │   Node 1        │◄──►│   Node 2        │◄──►│   Node 3        │ │                 │    │                 │    │                 │ │ • Distributed   │    │ • Auto-scaling  │    │ • Multi-region  │ │ • Consistent    │    │ • Survivable    │    │ • Performant    │ └─────────────────┘    └─────────────────┘    └─────────────────┘## Schema Design

### Core Tables

**Users Table:**
```sql
CREATE TABLE users (
    user_id VARCHAR(50) PRIMARY KEY,
    age INTEGER,
    location_country VARCHAR(2),
    signup_date DATE,
    verification_status VARCHAR(20),
    income_bracket VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_users_country ON users(location_country);
CREATE INDEX idx_users_age_bracket ON users(age, income_bracket);
CREATE INDEX idx_users_signup_date ON users(signup_date);Transactions Table:CREATE TABLE transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) REFERENCES users(user_id),
    amount DECIMAL(10,2),
    currency VARCHAR(3),
    merchant VARCHAR(100),
    category VARCHAR(50),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Partitioning by date for performance
CREATE TABLE transactions_2024_08 PARTITION OF transactions
    FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');

-- Indexes
CREATE INDEX idx_transactions_user_id ON transactions(user_id);
CREATE INDEX idx_transactions_created_at ON transactions(created_at);
CREATE INDEX idx_transactions_merchant ON transactions(merchant);Risk Scores Table:CREATE TABLE risk_scores (
    user_id VARCHAR(50) PRIMARY KEY REFERENCES users(user_id),
    risk_score DECIMAL(5,3),
    fraud_probability DECIMAL(5,3),
    payment_delays INTEGER,
    device_trust_score DECIMAL(5,3),
    credit_utilization DECIMAL(5,3),
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for risk-based queries
CREATE INDEX idx_risk_scores_risk_score ON risk_scores(risk_score);
CREATE INDEX idx_risk_scores_calculated_at ON risk_scores(calculated_at);Feature ViewsUser Features View:CREATE VIEW user_features AS
SELECT 
    u.user_id,
    u.age,
    u.location_country,
    u.verification_status,
    u.income_bracket,
    EXTRACT(DAYS FROM (CURRENT_DATE - u.signup_date)) as days_since_signup
FROM users u;Transaction Features View:CREATE VIEW transaction_features AS
SELECT 
    t.user_id,
    COUNT(*) as total_orders,
    AVG(t.amount) as avg_order_value,
    SUM(t.amount) as total_spent,
    MAX(t.created_at) as last_order_date,
    EXTRACT(DAYS FROM (CURRENT_TIMESTAMP - MAX(t.created_at))) as last_order_days,
    MODE() WITHIN GROUP (ORDER BY t.category) as favorite_category
FROM transactions t
WHERE t.status = 'completed'
GROUP BY t.user_id;PostgreSQL OptimizationPerformance Configurationpostgresql.conf optimizations:-- Memory settings
shared_buffers = 256MB          # 25% of RAM
effective_cache_size = 1GB      # 75% of RAM
work_mem = 4MB                  # Per-connection memory

-- Connection settings  
max_connections = 100           # Adjust based on workload
max_prepared_transactions = 100 # For 2PC if needed

-- Write-ahead logging
wal_buffers = 16MB
checkpoint_completion_target = 0.7
wal_writer_delay = 200ms

-- Query planning
random_page_cost = 1.1          # SSD storage
effective_io_concurrency = 200  # SSD parallelismIndex Strategy:-- Query-specific indexes
CREATE INDEX CONCURRENTLY idx_transactions_user_amount 
ON transactions(user_id, amount DESC);

-- Partial indexes for common filters
CREATE INDEX CONCURRENTLY idx_users_verified
ON users(user_id) WHERE verification_status = 'verified';

-- Composite indexes for complex queries
CREATE INDEX CONCURRENTLY idx_risk_user_score_calc
ON risk_scores(user_id, risk_score, calculated_at DESC);Query OptimizationEfficient Feature Queries:-- Optimized user feature query
SELECT 
    u.user_id,
    u.age, u.location_country, u.verification_status,
    tf.total_orders, tf.avg_order_value, tf.last_order_days,
    rs.risk_score, rs.fraud_probability, rs.payment_delays
FROM users u
LEFT JOIN transaction_features tf ON u.user_id = tf.user_id  
LEFT JOIN risk_scores rs ON u.user_id = rs.user_id
WHERE u.user_id = $1;

-- Batch query optimization
SELECT 
    u.user_id,
    jsonb_build_object(
        'age', u.age,
        'country', u.location_country,
        'verified', u.verification_status = 'verified'
    ) as user_features,
    jsonb_build_object(
        'orders', COALESCE(tf.total_orders, 0),
        'avg_amount', COALESCE(tf.avg_order_value, 0),
        'last_order_days', COALESCE(tf.last_order_days, 999)
    ) as transaction_features
FROM users u
LEFT JOIN transaction_features tf ON u.user_id = tf.user_id
WHERE u.user_id = ANY($1::text[]);CockroachDB Migration StrategyWhy CockroachDB?Benefits over PostgreSQL:Horizontal scaling: Add nodes without downtimeDistributed transactions: ACID across multiple nodesAutomatic failover: No single point of failureMulti-region support: Data locality and compliancePostgreSQL compatibility: Minimal code changesMigration PhasesPhase 1: Preparation (Month 1)-- 1. Schema compatibility check
-- CockroachDB-specific adjustments needed:

-- Change SERIAL to UUID for better distribution
ALTER TABLE users ALTER COLUMN user_id SET DEFAULT gen_random_uuid();

-- Add explicit primary keys where missing
ALTER TABLE user_sessions ADD COLUMN id UUID DEFAULT gen_random_uuid() PRIMARY KEY;

-- Optimize for distributed storage
ALTER TABLE transactions SPLIT AT VALUES 
  ('2024-01-01'), ('2024-04-01'), ('2024-07-01'), ('2024-10-01');Phase 2: Dual-Write Setup (Month 2)# Database abstraction layer
class DatabaseManager:
    def __init__(self):
        self.postgres = PostgreSQLConnection()
        self.cockroachdb = CockroachDBConnection()
        self.dual_write_enabled = os.getenv('DUAL_WRITE_ENABLED', 'false') == 'true'
    
    def write_user_features(self, user_data):
        # Write to PostgreSQL (primary)
        self.postgres.write(user_data)
        
        # Optionally write to CockroachDB
        if self.dual_write_enabled:
            try:
                self.cockroachdb.write(user_data)
            except Exception as e:
                logger.warning(f"CockroachDB write failed: {e}")Phase 3: Data Migration (Month 3)# Migration script
#!/bin/bash
set -e

echo "Starting PostgreSQL to CockroachDB migration..."

# 1. Export data from PostgreSQL
pg_dump --data-only --inserts feature_store > postgres_data.sql

# 2. Transform for CockroachDB compatibility  
python transform_sql.py postgres_data.sql > cockroach_data.sql

# 3. Import to CockroachDB
cockroach sql --database=feature_store < cockroach_data.sql

# 4. Verify data integrity
python verify_migration.py

echo "Migration completed successfully"Phase 4: Traffic Migration (Month 4)# Gradual traffic shifting
class TrafficSplitter:
    def __init__(self):
        self.cockroach_percentage = int(os.getenv('COCKROACH_TRAFFIC_PERCENT', '0'))
    
    def get_database_connection(self, user_id):
        # Use consistent hashing for gradual migration
        hash_value = hash(user_id) % 100
        
        if hash_value < self.cockroach_percentage:
            return self.cockroachdb_connection
        else:
            return self.postgresql_connectionCockroachDB Schema OptimizationsDistributed-Friendly Schema:-- Optimize table distribution
CREATE TABLE users (
    user_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    region VARCHAR(2),  -- Add region for locality
    age INTEGER,
    location_country VARCHAR(2),
    -- ... other fields
) PARTITION BY LIST (region);

-- Create regional partitions
ALTER TABLE users PARTITION region_us VALUES IN ('US');
ALTER TABLE users PARTITION region_eu VALUES IN ('EU', 'UK', 'DE', 'FR');
ALTER TABLE users PARTITION region_mena VALUES IN ('AE', 'SA', 'KW');

-- Optimize for distributed queries
CREATE INDEX idx_users_region_country ON users(region, location_country) 
STORING (age, verification_status);Transaction Isolation:-- Use appropriate isolation levels
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- Batch operations for better performance
INSERT INTO risk_scores (user_id, risk_score, calculated_at)
VALUES 
  ($1, $2, NOW()),
  ($3, $4, NOW()),
  -- ... batch insert
ON CONFLICT (user_id) DO UPDATE SET
  risk_score = EXCLUDED.risk_score,
  calculated_at = EXCLUDED.calculated_at;

COMMIT;BigQuery IntegrationData Pipeline ArchitecturePostgreSQL/CockroachDB → Debezium → Kafka → BigQuery
                                    ↓
                             Airflow ETL JobsBigQuery Schema:-- Partitioned table for efficient queries
CREATE TABLE `feature_store.user_transactions`
(
  user_id STRING,
  transaction_id STRING,
  amount NUMERIC,
  currency STRING,
  merchant STRING,
  category STRING,
  created_at TIMESTAMP
)
PARTITION BY DATE(created_at)
CLUSTER BY user_id, merchant;

-- Materialized view for feature aggregation
CREATE MATERIALIZED VIEW `feature_store.user_features_mv` AS
SELECT 
  user_id,
  COUNT(*) as total_transactions,
  AVG(amount) as avg_transaction_amount,
  ARRAY_AGG(DISTINCT merchant LIMIT 5) as top_merchants,
  DATE_DIFF(CURRENT_DATE(), MAX(DATE(created_at)), DAY) as days_since_last_transaction
FROM `feature_store.user_transactions`
WHERE DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
GROUP BY user_id;Performance MonitoringKey MetricsPostgreSQL Monitoring:-- Connection monitoring
SELECT count(*) as active_connections,
       state,
       wait_event_type
FROM pg_stat_activity 
WHERE state IS NOT NULL
GROUP BY state, wait_event_type;

-- Query performance
SELECT query,
       mean_exec_time,
       calls,
       total_exec_time
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 10;

-- Index usage
SELECT schemaname, tablename, indexname, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY idx_tup_read DESC;CockroachDB Monitoring:-- Node health
SHOW CLUSTER QUERIES;

-- Hot ranges identification
SELECT range_id, start_key, end_key, queries_per_second
FROM crdb_internal.hot_ranges
ORDER BY queries_per_second DESC;

-- Replication metrics
SELECT range_id, replica_id, store_id, is_voter
FROM crdb_internal.ranges_no_leases
WHERE database_name = 'feature_store';Performance BenchmarksExpected Performance (PostgreSQL):Single user query: <5msBatch query (100 users): <50msWrite throughput: 1000+ TPSConcurrent connections: 100+Expected Performance (CockroachDB):Single user query: <10msBatch query (100 users): <100msWrite throughput: 5000+ TPSConcurrent connections: 1000+Backup and RecoveryPostgreSQL Backup Strategy# Automated daily backups
#!/bin/bash
BACKUP_DIR="/backups/$(date +%Y%m%d)"
mkdir -p $BACKUP_DIR

# Full database backup
pg_dump feature_store | gzip > $BACKUP_DIR/feature_store_full.sql.gz

# WAL archiving for point-in-time recovery
archive_command = 'cp %p /backups/wal/%f'CockroachDB Backup Strategy-- Full cluster backup
BACKUP DATABASE feature_store TO 'gs://feature-store-backups/full';

-- Incremental backup
BACKUP DATABASE feature_store TO 'gs://feature-store-backups/inc'
AS OF SYSTEM TIME '-1m' INCREMENTAL FROM 'gs://feature-store-backups/full';

-- Scheduled backups
CREATE SCHEDULE feature_store_backup
FOR BACKUP DATABASE feature_store INTO 'gs://feature-store-backups'
  RECURRING '@daily'
  FULL BACKUP '@weekly'
  WITH SCHEDULE OPTIONS first_run = 'now';This database strategy provides a solid foundation for current operations while preparing for future scale requirements through the PostgreSQL to CockroachDB migration path.