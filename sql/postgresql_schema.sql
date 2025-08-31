-- PostgreSQL Schema for Feature Store
-- Optimized for OLTP workloads with proper indexing

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Users base table
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(100) PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    age INTEGER CHECK (age >= 18 AND age <= 100),
    location_country CHAR(2),
    location_city VARCHAR(100),
    account_verified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- User features table
CREATE TABLE IF NOT EXISTS user_features (
    user_id VARCHAR(100) PRIMARY KEY REFERENCES users(user_id),
    age INTEGER CHECK (age >= 18 AND age <= 100),
    location_country CHAR(2),
    location_city VARCHAR(100),
    total_orders INTEGER DEFAULT 0 CHECK (total_orders >= 0),
    avg_order_value DECIMAL(10,2) DEFAULT 0.00 CHECK (avg_order_value >= 0),
    days_since_first_order INTEGER CHECK (days_since_first_order >= 0),
    preferred_payment_method VARCHAR(50),
    account_verified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Transaction features table
CREATE TABLE IF NOT EXISTS transaction_features (
    user_id VARCHAR(100) PRIMARY KEY REFERENCES users(user_id),
    total_transactions_30d INTEGER DEFAULT 0 CHECK (total_transactions_30d >= 0),
    total_amount_30d DECIMAL(12,2) DEFAULT 0.00 CHECK (total_amount_30d >= 0),
    avg_transaction_amount DECIMAL(10,2) DEFAULT 0.00 CHECK (avg_transaction_amount >= 0),
    max_transaction_amount DECIMAL(10,2) DEFAULT 0.00 CHECK (max_transaction_amount >= 0),
    transactions_declined_30d INTEGER DEFAULT 0 CHECK (transactions_declined_30d >= 0),
    unique_merchants_30d INTEGER DEFAULT 0 CHECK (unique_merchants_30d >= 0),
    weekend_transaction_ratio DECIMAL(5,4) DEFAULT 0.0000 CHECK (weekend_transaction_ratio >= 0 AND weekend_transaction_ratio <= 1),
    night_transaction_ratio DECIMAL(5,4) DEFAULT 0.0000 CHECK (night_transaction_ratio >= 0 AND night_transaction_ratio <= 1),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Risk features table
CREATE TABLE IF NOT EXISTS risk_features (
    user_id VARCHAR(100) PRIMARY KEY REFERENCES users(user_id),
    credit_utilization_ratio DECIMAL(5,4) DEFAULT 0.0000 CHECK (credit_utilization_ratio >= 0 AND credit_utilization_ratio <= 1),
    payment_delays_30d INTEGER DEFAULT 0 CHECK (payment_delays_30d >= 0),
    payment_delays_90d INTEGER DEFAULT 0 CHECK (payment_delays_90d >= 0),
    failed_payments_count INTEGER DEFAULT 0 CHECK (failed_payments_count >= 0),
    device_changes_30d INTEGER DEFAULT 0 CHECK (device_changes_30d >= 0),
    login_locations_30d INTEGER DEFAULT 0 CHECK (login_locations_30d >= 0),
    velocity_alerts_30d INTEGER DEFAULT 0 CHECK (velocity_alerts_30d >= 0),
    risk_score DECIMAL(5,4) DEFAULT 0.0000 CHECK (risk_score >= 0 AND risk_score <= 1),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Supporting tables for feature computation
CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id VARCHAR(100) NOT NULL REFERENCES users(user_id),
    merchant_id VARCHAR(100) NOT NULL,
    amount DECIMAL(10,2) NOT NULL CHECK (amount >= 0),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    payment_method VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id VARCHAR(100) NOT NULL REFERENCES users(user_id),
    order_id UUID REFERENCES orders(order_id),
    amount DECIMAL(10,2) NOT NULL CHECK (amount >= 0),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    due_date TIMESTAMP WITH TIME ZONE,
    payment_date TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_logins (
    login_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id VARCHAR(100) NOT NULL REFERENCES users(user_id),
    device_id VARCHAR(255),
    ip_address INET,
    user_agent TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS velocity_alerts (
    alert_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id VARCHAR(100) NOT NULL REFERENCES users(user_id),
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) DEFAULT 'medium',
    message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS credit_reports (
    report_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id VARCHAR(100) NOT NULL REFERENCES users(user_id),
    credit_utilization_ratio DECIMAL(5,4) CHECK (credit_utilization_ratio >= 0 AND credit_utilization_ratio <= 1),
    credit_score INTEGER CHECK (credit_score >= 300 AND credit_score <= 850),
    report_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS risk_scores (
    score_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id VARCHAR(100) NOT NULL REFERENCES users(user_id),
    risk_score DECIMAL(5,4) CHECK (risk_score >= 0 AND risk_score <= 1),
    model_version VARCHAR(20),
    computed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_user_features_updated_at ON user_features(updated_at);
CREATE INDEX IF NOT EXISTS idx_transaction_features_updated_at ON transaction_features(updated_at);
CREATE INDEX IF NOT EXISTS idx_risk_features_updated_at ON risk_features(updated_at);

CREATE INDEX IF NOT EXISTS idx_orders_user_id_created_at ON orders(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_orders_merchant_id ON orders(merchant_id);
CREATE INDEX IF NOT EXISTS idx_orders_status_created_at ON orders(status, created_at);

CREATE INDEX IF NOT EXISTS idx_payments_user_id_status ON payments(user_id, status);
CREATE INDEX IF NOT EXISTS idx_payments_due_date ON payments(due_date);
CREATE INDEX IF NOT EXISTS idx_payments_payment_date ON payments(payment_date);

CREATE INDEX IF NOT EXISTS idx_user_logins_user_id_created_at ON user_logins(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_user_logins_device_id ON user_logins(device_id);
CREATE INDEX IF NOT EXISTS idx_user_logins_ip_address ON user_logins(ip_address);

CREATE INDEX IF NOT EXISTS idx_velocity_alerts_user_id_created_at ON velocity_alerts(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_velocity_alerts_type_created_at ON velocity_alerts(alert_type, created_at);

-- Update triggers for updated_at columns
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_user_features_updated_at BEFORE UPDATE ON user_features FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_transaction_features_updated_at BEFORE UPDATE ON transaction_features FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_risk_features_updated_at BEFORE UPDATE ON risk_features FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON orders FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_payments_updated_at BEFORE UPDATE ON payments FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();