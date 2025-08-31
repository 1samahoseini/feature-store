-- Transaction Features Computation Query
-- Computes transaction-based behavioral features

WITH transaction_stats AS (
  SELECT 
    o.user_id,
    -- 30-day transaction metrics
    COUNT(*) FILTER (WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days') as total_transactions_30d,
    SUM(o.amount) FILTER (WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days') as total_amount_30d,
    AVG(o.amount) FILTER (WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days') as avg_transaction_amount,
    MAX(o.amount) FILTER (WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days') as max_transaction_amount,
    
    -- Declined transactions
    COUNT(*) FILTER (WHERE o.status = 'declined' AND o.created_at >= CURRENT_DATE - INTERVAL '30 days') as transactions_declined_30d,
    
    -- Merchant diversity
    COUNT(DISTINCT o.merchant_id) FILTER (WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days') as unique_merchants_30d,
    
    -- Time-based patterns
    COUNT(*) FILTER (
      WHERE EXTRACT(DOW FROM o.created_at) IN (0, 6) -- Sunday = 0, Saturday = 6
      AND o.created_at >= CURRENT_DATE - INTERVAL '30 days'
    )::FLOAT / NULLIF(
      COUNT(*) FILTER (WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days'), 0
    ) as weekend_transaction_ratio,
    
    COUNT(*) FILTER (
      WHERE EXTRACT(HOUR FROM o.created_at) BETWEEN 22 AND 6
      AND o.created_at >= CURRENT_DATE - INTERVAL '30 days'
    )::FLOAT / NULLIF(
      COUNT(*) FILTER (WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days'), 0
    ) as night_transaction_ratio
    
  FROM orders o
  WHERE o.created_at >= CURRENT_DATE - INTERVAL '31 days'  -- Get slightly more data for calculations
  GROUP BY o.user_id
),
active_users AS (
  -- Only include users who have had transactions or updates recently
  SELECT DISTINCT user_id
  FROM users 
  WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
  
  UNION
  
  SELECT DISTINCT user_id
  FROM orders
  WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
)

SELECT 
  au.user_id,
  COALESCE(ts.total_transactions_30d, 0) as total_transactions_30d,
  COALESCE(ts.total_amount_30d, 0.00) as total_amount_30d,
  COALESCE(ts.avg_transaction_amount, 0.00) as avg_transaction_amount,
  COALESCE(ts.max_transaction_amount, 0.00) as max_transaction_amount,
  COALESCE(ts.transactions_declined_30d, 0) as transactions_declined_30d,
  COALESCE(ts.unique_merchants_30d, 0) as unique_merchants_30d,
  COALESCE(ts.weekend_transaction_ratio, 0.0000) as weekend_transaction_ratio,
  COALESCE(ts.night_transaction_ratio, 0.0000) as night_transaction_ratio,
  CURRENT_TIMESTAMP as created_at,
  CURRENT_TIMESTAMP as updated_at
FROM active_users au
LEFT JOIN transaction_stats ts ON au.user_id = ts.user_id;