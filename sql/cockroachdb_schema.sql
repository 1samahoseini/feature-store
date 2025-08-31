-- CockroachDB Schema for Feature Store
-- Optimized for distributed scalability

-- Users base table
CREATE TABLE IF NOT EXISTS users (
    user_id STRING(100) PRIMARY KEY,
    email STRING(255) UNIQUE NOT NULL,
    age INT2 CHECK (age >= 18 AND age <= 100),
    location_country STRING(2),
    location_city STRING(100),
    account_verified BOOL DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT current_timestamp(),
    updated_at TIMESTAMPTZ DEFAULT current_timestamp(),
    
    INDEX idx_users_location (location_country, location_city),
    INDEX idx_users_created_at (created_at),
    INDEX idx_users_updated_at (updated_at)
);

-- User features table
CREATE TABLE IF NOT EXISTS user_features (
    user_id STRING(100) PRIMARY KEY REFERENCES users(user_id),
    age INT2 CHECK (age >= 18 AND age <= 100),
    location_country STRING(2),
    location_city STRING(100),
    total_orders INT4 DEFAULT 0 CHECK (total_orders >= 0),
    avg_order_value DECIMAL(10,2) DEFAULT 0.00 CHECK (avg_order_value >= 0),
    days_since_first_order INT4 CHECK (days_since_first_order >= 0),
    preferred_payment_method STRING(50),
    account_verified BOOL DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT current_timestamp(),
    updated_at TIMESTAMPTZ DEFAULT current_timestamp(),
    
    INDEX idx_user_features_updated_at (updated_at),
    INDEX idx_user_features_location (location_country, location_city),
    INDEX idx_user_features_orders (total_orders, avg_order_value)
);

-- Transaction features table
CREATE TABLE IF NOT EXISTS transaction_features (
    user_id STRING(100) PRIMARY KEY REFERENCES users(user_id),
    total_transactions_30d INT4 DEFAULT 0 CHECK (total_transactions_30d >= 0),
    total_amount_30d DECIMAL(12,2) DEFAULT 0.00 CHECK (total_amount_30d >= 0),
    avg_transaction_amount DECIMAL(10,2) DEFAULT 0.00 CHECK (avg_transaction_amount >= 0),
    max_transaction_amount DECIMAL(10,2) DEFAULT 0.00 CHECK (max_transaction_amount >= 0),
    transactions_declined_30d INT4 DEFAULT 0 CHECK (transactions_declined_30d >= 0),
    unique_merchants_30d INT4 DEFAULT 0 CHECK (unique_merchants_30d >= 0),
    weekend_transaction_ratio DECIMAL(5,4) DEFAULT 0.0000 CHECK (weekend_transaction_ratio >= 0 AND weekend_transaction_ratio <= 1),
    night_transaction_ratio DECIMAL(5,4) DEFAULT 0.0000 CHECK (night_transaction_ratio >= 0 AND night_transaction_ratio <= 1),
    created_at TIMESTAMPTZ DEFAULT current_timestamp(),
    updated_at TIMESTAMPTZ DEFAULT current_timestamp(),
    
    INDEX idx_transaction_features_updated_at (updated_at),
    INDEX idx_transaction_features_amounts (total_amount_30d, avg_transaction_amount),
    INDEX idx_transaction_features_counts (total_transactions_30d, transactions_declined_30d)
);

-- Risk features table
CREATE TABLE IF NOT EXISTS risk_features (
    user_id STRING(100) PRIMARY KEY REFERENCES users(user_id),
    credit_utilization_ratio DECIMAL(5,4) DEFAULT 0.0000 CHECK (credit_utilization_ratio >= 0 AND credit_utilization_ratio <= 1),
    payment_delays_30d INT4 DEFAULT 0 CHECK (payment_delays_30d >= 0),
    payment_delays_90d INT4 DEFAULT 0 CHECK (payment_delays_90d >= 0),
    failed_payments_count INT4 DEFAULT 0 CHECK (failed_payments_count >= 0),
    device_changes_30d INT4 DEFAULT 0 CHECK (device_changes_30d >= 0),
    login_locations_30d INT4 DEFAULT 0 CHECK (login_locations_30d >= 0),
    velocity_alerts_30d INT4 DEFAULT 0 CHECK (velocity_alerts_30d >= 0),
    risk_score DECIMAL(5,4) DEFAULT 0.0000 CHECK (risk_score >= 0 AND risk_score <= 1),
    created_at TIMESTAMPTZ DEFAULT current_timestamp(),
    updated_at TIMESTAMPTZ DEFAULT current_timestamp(),
    
    INDEX idx_risk_features_updated_at (updated_at),
    INDEX idx_risk_features_risk_score (risk_score),
    INDEX idx_risk_features_credit_util (credit_utilization_ratio),
    INDEX idx_risk_features_delays (payment_delays_30d, payment_delays_90d)
);

-- Supporting tables for feature computation
CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id STRING(100) NOT NULL REFERENCES users(user_id),
    merchant_id STRING(100) NOT NULL,
    amount DECIMAL(10,2) NOT NULL CHECK (amount >= 0),
    status STRING(20) NOT NULL DEFAULT 'pending',
    payment_method STRING(50),
    created_at TIMESTAMPTZ DEFAULT current_timestamp(),
    updated_at TIMESTAMPTZ DEFAULT current_timestamp(),
    
    INDEX idx_orders_user_id_created_at (user_id, created_at),
    INDEX idx_orders_merchant_id (merchant_id),
    INDEX idx_orders_status_created_at (status, created_at),
    INDEX idx_orders_amount (amount)
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id STRING(100) NOT NULL REFERENCES users(user_id),
    order_id UUID REFERENCES orders(order_id),
    amount DECIMAL(10,2) NOT NULL CHECK (amount >= 0),
    status STRING(20) NOT NULL DEFAULT 'pending',
    due_date TIMESTAMPTZ,
    payment_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT current_timestamp(),
    updated_at TIMESTAMPTZ DEFAULT current_timestamp(),
    
    INDEX idx_payments_user_id_status (user_id, status),
    INDEX idx_payments_due_date (due_date),
    INDEX idx_payments_payment_date (payment_date),
    INDEX idx_payments_status_created_at (status, created_at)
);

CREATE TABLE IF NOT EXISTS user_logins (
    login_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id STRING(100) NOT NULL REFERENCES users(user_id),
    device_id STRING(255),
    ip_address INET,
    user_agent STRING,
    created_at TIMESTAMPTZ DEFAULT current_timestamp(),
    
    INDEX idx_user_logins_user_id_created_at (user_id, created_at),
    INDEX idx_user_logins_device_id (device_id),
    INDEX idx_user_logins_ip_address (ip_address)
);

CREATE TABLE IF NOT EXISTS velocity_alerts (
    alert_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id STRING(100) NOT NULL REFERENCES users(user_id),
    alert_type STRING(50) NOT NULL,
    severity STRING(20) DEFAULT 'medium',
    message STRING,
    created_at TIMESTAMPTZ DEFAULT current_timestamp(),
    
    INDEX idx_velocity_alerts_user_id_created_at (user_id, created_at),
    INDEX idx_velocity_alerts_type_created_at (alert_type, created_at)
);

CREATE TABLE IF NOT EXISTS credit_reports (
    report_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id STRING(100) NOT NULL REFERENCES users(user_id),
    credit_utilization_ratio DECIMAL(5,4) CHECK (credit_utilization_ratio >= 0 AND credit_utilization_ratio <= 1),
    credit_score INT2 CHECK (credit_score >= 300 AND credit_score <= 850),
    report_date TIMESTAMPTZ DEFAULT current_timestamp(),
    created_at TIMESTAMPTZ DEFAULT current_timestamp(),
    
    INDEX idx_credit_reports_user_id_date (user_id, report_date),
    INDEX idx_credit_reports_score (credit_score)
);

CREATE TABLE IF NOT EXISTS risk_scores (
    score_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id STRING(100) NOT NULL REFERENCES users(user_id),
    risk_score DECIMAL(5,4) CHECK (risk_score >= 0 AND risk_score <= 1),
    model_version STRING(20),
    computed_at TIMESTAMPTZ DEFAULT current_timestamp(),
    created_at TIMESTAMPTZ DEFAULT current_timestamp(),
    
    INDEX idx_risk_scores_user_id_computed (user_id, computed_at),
    INDEX idx_risk_scores_score (risk_score),
    INDEX idx_risk_scores_version (model_version)
);