-- BigQuery Schema for Feature Store Data Warehouse
-- Optimized for analytical queries and reporting

-- User features table
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.user_features` (
  user_id STRING NOT NULL,
  age INT64,
  location_country STRING,
  location_city STRING,
  total_orders INT64,
  avg_order_value NUMERIC(10,2),
  days_since_first_order INT64,
  preferred_payment_method STRING,
  account_verified BOOL,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  
  -- Partitioning for performance
  _partitioning_date DATE GENERATED ALWAYS AS (DATE(updated_at)) STORED
)
PARTITION BY _partitioning_date
CLUSTER BY user_id, location_country
OPTIONS (
  description = "User demographic and behavioral features for analytics",
  partition_expiration_days = 730  -- 2 years retention
);

-- Transaction features table
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.transaction_features` (
  user_id STRING NOT NULL,
  total_transactions_30d INT64,
  total_amount_30d NUMERIC(12,2),
  avg_transaction_amount NUMERIC(10,2),
  max_transaction_amount NUMERIC(10,2),
  transactions_declined_30d INT64,
  unique_merchants_30d INT64,
  weekend_transaction_ratio NUMERIC(5,4),
  night_transaction_ratio NUMERIC(5,4),
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  
  _partitioning_date DATE GENERATED ALWAYS AS (DATE(updated_at)) STORED
)
PARTITION BY _partitioning_date
CLUSTER BY user_id, total_amount_30d
OPTIONS (
  description = "Transaction-based features for analytics",
  partition_expiration_days = 730
);

-- Risk features table
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.risk_features` (
  user_id STRING NOT NULL,
  credit_utilization_ratio NUMERIC(5,4),
  payment_delays_30d INT64,
  payment_delays_90d INT64,
  failed_payments_count INT64,
  device_changes_30d INT64,
  login_locations_30d INT64,
  velocity_alerts_30d INT64,
  risk_score NUMERIC(5,4),
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  
  _partitioning_date DATE GENERATED ALWAYS AS (DATE(updated_at)) STORED
)
PARTITION BY _partitioning_date
CLUSTER BY user_id, risk_score
OPTIONS (
  description = "Risk assessment features for analytics",
  partition_expiration_days = 730
);

-- Analytical views for reporting
CREATE VIEW IF NOT EXISTS `{project_id}.{dataset}.user_risk_summary` AS
SELECT 
  uf.user_id,
  uf.location_country,
  uf.total_orders,
  uf.avg_order_value,
  tf.total_transactions_30d,
  tf.total_amount_30d,
  rf.risk_score,
  rf.credit_utilization_ratio,
  CASE 
    WHEN rf.risk_score <= 0.3 THEN 'Low'
    WHEN rf.risk_score <= 0.7 THEN 'Medium'
    ELSE 'High'
  END as risk_category,
  uf.updated_at as last_updated
FROM `{project_id}.{dataset}.user_features` uf
LEFT JOIN `{project_id}.{dataset}.transaction_features` tf ON uf.user_id = tf.user_id
LEFT JOIN `{project_id}.{dataset}.risk_features` rf ON uf.user_id = rf.user_id
WHERE uf._partitioning_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);

-- Feature aggregation view
CREATE VIEW IF NOT EXISTS `{project_id}.{dataset}.feature_completeness` AS
SELECT 
  'user_features' as table_name,
  COUNT(*) as total_records,
  COUNTIF(age IS NOT NULL) as age_completeness,
  COUNTIF(location_country IS NOT NULL) as location_completeness,
  COUNTIF(total_orders > 0) as orders_completeness,
  AVG(total_orders) as avg_total_orders,
  MAX(updated_at) as last_update
FROM `{project_id}.{dataset}.user_features`
WHERE _partitioning_date = CURRENT_DATE()

UNION ALL

SELECT 
  'transaction_features' as table_name,
  COUNT(*) as total_records,
  COUNTIF(total_transactions_30d > 0) as transactions_completeness,
  COUNTIF(total_amount_30d > 0) as amount_completeness,
  COUNTIF(unique_merchants_30d > 0) as merchants_completeness,
  AVG(total_transactions_30d) as avg_transactions,
  MAX(updated_at) as last_update
FROM `{project_id}.{dataset}.transaction_features`
WHERE _partitioning_date = CURRENT_DATE()

UNION ALL

SELECT 
  'risk_features' as table_name,
  COUNT(*) as total_records,
  COUNTIF(risk_score > 0) as risk_score_completeness,
  COUNTIF(credit_utilization_ratio > 0) as credit_util_completeness,
  COUNTIF(payment_delays_30d >= 0) as payment_delays_completeness,
  AVG(risk_score) as avg_risk_score,
  MAX(updated_at) as last_update
FROM `{project_id}.{dataset}.risk_features`
WHERE _partitioning_date = CURRENT_DATE();