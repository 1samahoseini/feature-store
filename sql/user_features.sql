-- User Features Computation Query
-- Computes demographic and behavioral features for users

WITH user_orders_stats AS (
  SELECT 
    o.user_id,
    COUNT(o.order_id) as total_orders,
    AVG(o.amount) as avg_order_value,
    MIN(o.created_at) as first_order_date,
    MAX(o.created_at) as last_order_date,
    MODE() WITHIN GROUP (ORDER BY o.payment_method) as preferred_payment_method
  FROM orders o
  WHERE o.status IN ('completed', 'fulfilled')
  GROUP BY o.user_id
),
user_base_info AS (
  SELECT 
    u.user_id,
    u.age,
    u.location_country,
    u.location_city,
    u.account_verified,
    u.created_at as user_created_at
  FROM users u
  WHERE u.updated_at >= CURRENT_DATE - INTERVAL '1 day'  -- Only recently updated users
)

SELECT 
  ubi.user_id,
  ubi.age,
  ubi.location_country,
  ubi.location_city,
  COALESCE(uos.total_orders, 0) as total_orders,
  COALESCE(uos.avg_order_value, 0.00) as avg_order_value,
  CASE 
    WHEN uos.first_order_date IS NOT NULL 
    THEN EXTRACT(DAY FROM CURRENT_DATE - DATE(uos.first_order_date))::INTEGER
    ELSE NULL
  END as days_since_first_order,
  uos.preferred_payment_method,
  ubi.account_verified,
  CURRENT_TIMESTAMP as created_at,
  CURRENT_TIMESTAMP as updated_at
FROM user_base_info ubi
LEFT JOIN user_orders_stats uos ON ubi.user_id = uos.user_id
WHERE ubi.user_id IS NOT NULL;