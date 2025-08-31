-- Risk Features Computation Query
-- Computes risk assessment and behavioral risk indicators

WITH payment_history AS (
  SELECT 
    p.user_id,
    -- Payment delays
    COUNT(*) FILTER (
      WHERE p.payment_date > p.due_date 
      AND p.payment_date >= CURRENT_DATE - INTERVAL '30 days'
    ) as payment_delays_30d,
    
    COUNT(*) FILTER (
      WHERE p.payment_date > p.due_date 
      AND p.payment_date >= CURRENT_DATE - INTERVAL '90 days'
    ) as payment_delays_90d,
    
    -- Failed payments
    COUNT(*) FILTER (
      WHERE p.status = 'failed' 
      AND p.created_at >= CURRENT_DATE - INTERVAL '90 days'
    ) as failed_payments_count
    
  FROM payments p
  WHERE p.created_at >= CURRENT_DATE - INTERVAL '91 days'
  GROUP BY p.user_id
),
device_behavior AS (
  SELECT 
    ul.user_id,
    -- Device changes (unique devices in 30 days)
    COUNT(DISTINCT ul.device_id) FILTER (
      WHERE ul.created_at >= CURRENT_DATE - INTERVAL '30 days'
    ) as device_changes_30d,
    
    -- Location changes (unique IPs in 30 days)
    COUNT(DISTINCT ul.ip_address) FILTER (
      WHERE ul.created_at >= CURRENT_DATE - INTERVAL '30 days'
    ) as login_locations_30d
    
  FROM user_logins ul
  WHERE ul.created_at >= CURRENT_DATE - INTERVAL '31 days'
  GROUP BY ul.user_id
),
velocity_patterns AS (
  SELECT 
    va.user_id,
    COUNT(*) FILTER (
      WHERE va.alert_type = 'velocity' 
      AND va.created_at >= CURRENT_DATE - INTERVAL '30 days'
    ) as velocity_alerts_30d
    
  FROM velocity_alerts va
  WHERE va.created_at >= CURRENT_DATE - INTERVAL '31 days'
  GROUP BY va.user_id
),
credit_info AS (
  SELECT DISTINCT ON (cr.user_id)
    cr.user_id,
    cr.credit_utilization_ratio
  FROM credit_reports cr
  WHERE cr.report_date >= CURRENT_DATE - INTERVAL '30 days'
  ORDER BY cr.user_id, cr.report_date DESC
),
latest_risk_scores AS (
  SELECT DISTINCT ON (rs.user_id)
    rs.user_id,
    rs.risk_score
  FROM risk_scores rs
  WHERE rs.computed_at >= CURRENT_DATE - INTERVAL '7 days'
  ORDER BY rs.user_id, rs.computed_at DESC
),
active_risk_users AS (
  -- Users who need risk feature updates
  SELECT DISTINCT user_id
  FROM users 
  WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
  
  UNION
  
  SELECT DISTINCT user_id
  FROM payments
  WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
  
  UNION
  
  SELECT DISTINCT user_id
  FROM velocity_alerts
  WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
)

SELECT 
  aru.user_id,
  COALESCE(ci.credit_utilization_ratio, 0.0000) as credit_utilization_ratio,
  COALESCE(ph.payment_delays_30d, 0) as payment_delays_30d,
  COALESCE(ph.payment_delays_90d, 0) as payment_delays_90d,
  COALESCE(ph.failed_payments_count, 0) as failed_payments_count,
  COALESCE(db.device_changes_30d, 0) as device_changes_30d,
  COALESCE(db.login_locations_30d, 0) as login_locations_30d,
  COALESCE(vp.velocity_alerts_30d, 0) as velocity_alerts_30d,
  COALESCE(lrs.risk_score, 0.0000) as risk_score,
  CURRENT_TIMESTAMP as created_at,
  CURRENT_TIMESTAMP as updated_at
FROM active_risk_users aru
LEFT JOIN payment_history ph ON aru.user_id = ph.user_id
LEFT JOIN device_behavior db ON aru.user_id = db.user_id
LEFT JOIN velocity_patterns vp ON aru.user_id = vp.user_id
LEFT JOIN credit_info ci ON aru.user_id = ci.user_id
LEFT JOIN latest_risk_scores lrs ON aru.user_id = lrs.user_id;