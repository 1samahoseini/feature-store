# BNPL Feature Store - Monitoring & Observability

## Overview

Comprehensive monitoring and observability strategy for the BNPL Feature Store, covering metrics collection, alerting, logging, and performance analysis. The system uses Prometheus, Grafana, and structured logging for complete operational visibility.

## Monitoring Architecture

### Components Overview┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐ │  Application    │───▶│   Prometheus    │───▶│    Grafana      │ │  (Metrics)      │    │  (Collection)   │    │  (Visualization)│ └─────────────────┘    └─────────────────┘    └─────────────────┘ │                       │                       │ ▼                       ▼                       ▼ ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐ │     Logs        │    │    Alerts       │    │   Dashboards    │ │ (Structured)    │    │ (Slack/Email)   │    │   (Real-time)   │ └─────────────────┘    └─────────────────┘    └─────────────────┘### Metrics Collection Stack

**Application Metrics (Python):**
```python
from prometheus_client import Counter, Histogram, Gauge, Info
import time
import functools

# Core business metrics
FEATURE_REQUESTS = Counter(
    'feature_store_requests_total',
    'Total feature requests',
    ['user_type', 'feature_type', 'cache_hit']
)

FEATURE_REQUEST_DURATION = Histogram(
    'feature_store_request_duration_seconds',
    'Feature request duration',
    ['endpoint', 'cache_hit'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0]
)

CACHE_HIT_RATE = Gauge(
    'feature_store_cache_hit_rate',
    'Cache hit rate percentage'
)

ACTIVE_USERS = Gauge(
    'feature_store_active_users',
    'Number of unique active users in last 5 minutes'
)

DATABASE_CONNECTIONS = Gauge(
    'feature_store_db_connections',
    'Active database connections',
    ['pool', 'state']
)

# Performance decorator for automatic instrumentation
def monitor_performance(operation_name: str):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            
            try:
                result = await func(*args, **kwargs)
                
                # Record success metrics
                FEATURE_REQUESTS.labels(
                    user_type='standard',
                    feature_type=operation_name,
                    cache_hit=getattr(result, 'cache_hit', False)
                ).inc()
                
                return result
                
            except Exception as e:
                # Record error metrics
                ERROR_COUNTER.labels(
                    operation=operation_name,
                    error_type=type(e).__name__
                ).inc()
                raise
                
            finally:
                # Record duration
                duration = time.time() - start_time
                FEATURE_REQUEST_DURATION.labels(
                    endpoint=operation_name,
                    cache_hit=getattr(result, 'cache_hit', False)
                ).observe(duration)
        
        return wrapper
    return decorator

# Usage example
@monitor_performance('get_user_features')
async def get_user_features(user_id: str):
    # Function implementation
    passKey Performance Indicators (KPIs)Service Level Indicators (SLIs)Latency Metrics:# SLI: Request latency
LATENCY_SLI = Histogram(
    'feature_store_latency_sli',
    'Request latency SLI',
    ['service', 'operation'],
    buckets=[0.01, 0.02, 0.03, 0.04, 0.05, 0.1, 0.2, 0.5, 1.0]  # Focus on sub-40ms
)

# SLI: Cache performance
CACHE_PERFORMANCE_SLI = Gauge(
    'feature_store_cache_sli',
    'Cache performance SLI',
    ['cache_type']
)

# SLI: Availability
AVAILABILITY_SLI = Gauge(
    'feature_store_availability_sli',
    'Service availability SLI'
)Business Metrics:# Business KPIs
CREDIT_DECISIONS = Counter(
    'bnpl_credit_decisions_total',
    'Total credit decisions made',
    ['decision', 'risk_level', 'amount_bucket']
)

APPROVAL_RATE = Gauge(
    'bnpl_approval_rate',
    'Credit approval rate percentage'
)

PROCESSING_VOLUME = Counter(
    'bnpl_processing_volume_total',
    'Total processing volume',
    ['currency']
)

AVERAGE_DECISION_TIME = Histogram(
    'bnpl_decision_time_seconds',
    'Time to make credit decision',
    buckets=[0.01, 0.02, 0.03, 0.04, 0.05, 0.1]
)Grafana DashboardsMain Dashboard ConfigurationFeature Store Overview Dashboard:{
  "dashboard": {
    "id": null,
    "title": "BNPL Feature Store - Overview",
    "tags": ["feature-store", "bnpl", "production"],
    "timezone": "UTC",
    "panels": [
      {
        "id": 1,
        "title": "Request Rate (RPS)",
        "type": "stat",
        "targets": [
          {
            "expr": "rate(feature_store_requests_total[5m])",
            "legendFormat": "{{endpoint}}"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "color": {
              "mode": "thresholds"
            },
            "thresholds": {
              "values": [
                {"color": "green", "value": 0},
                {"color": "yellow", "value": 8000},
                {"color": "red", "value": 12000}
              ]
            }
          }
        }
      },
      {
        "id": 2,
        "title": "Response Time P95",
        "type": "stat",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, rate(feature_store_request_duration_seconds_bucket[5m]))",
            "legendFormat": "P95 Latency"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "s",
            "thresholds": {
              "values": [
                {"color": "green", "value": 0},
                {"color": "yellow", "value": 0.035},
                {"color": "red", "value": 0.05}
              ]
            }
          }
        }
      },
      {
        "id": 3,
        "title": "Cache Hit Rate",
        "type": "stat",
        "targets": [
          {
            "expr": "feature_store_cache_hit_rate",
            "legendFormat": "Hit Rate %"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "percent",
            "thresholds": {
              "values": [
                {"color": "red", "value": 0},
                {"color": "yellow", "value": 85},
                {"color": "green", "value": 90}
              ]
            }
          }
        }
      }
    ]
  }
}Performance Deep Dive Dashboard:{
  "dashboard": {
    "title": "BNPL Feature Store - Performance Analysis",
    "panels": [
      {
        "title": "Request Latency Distribution",
        "type": "heatmap",
        "targets": [
          {
            "expr": "rate(feature_store_request_duration_seconds_bucket[5m])",
            "format": "heatmap",
            "legendFormat": "{{le}}"
          }
        ]
      },
      {
        "title": "Database Connection Pool",
        "type": "graph",
        "targets": [
          {
            "expr": "feature_store_db_connections",
            "legendFormat": "{{pool}}-{{state}}"
          }
        ]
      },
      {
        "title": "Memory Usage",
        "type": "graph", 
        "targets": [
          {
            "expr": "process_resident_memory_bytes",
            "legendFormat": "RSS Memory"
          },
          {
            "expr": "process_virtual_memory_bytes", 
            "legendFormat": "Virtual Memory"
          }
        ]
      }
    ]
  }
}Alerting StrategyCritical Alerts (Immediate Response)Prometheus Alert Rules:# prometheus_alerts.yml
groups:
- name: feature_store_critical
  rules:
  - alert: HighLatency
    expr: histogram_quantile(0.95, rate(feature_store_request_duration_seconds_bucket[5m])) > 0.04
    for: 2m
    labels:
      severity: critical
      team: data-platform
    annotations:
      summary: "Feature Store P95 latency too high"
      description: "P95 latency is {{ $value }}s, exceeding 40ms SLA"
      runbook_url: "https://wiki.company.com/runbooks/feature-store-latency"
  
  - alert: LowCacheHitRate
    expr: feature_store_cache_hit_rate < 85
    for: 5m
    labels:
      severity: critical
      team: data-platform
    annotations:
      summary: "Cache hit rate below threshold"
      description: "Cache hit rate is {{ $value }}%, below 85% threshold"
  
  - alert: HighErrorRate
    expr: rate(feature_store_requests_total{status=~"5.."}[5m]) / rate(feature_store_requests_total[5m]) > 0.01
    for: 1m
    labels:
      severity: critical
      team: data-platform
    annotations:
      summary: "High error rate detected"
      description: "Error rate is {{ $value | humanizePercentage }}"
  
  - alert: ServiceDown
    expr: up{job="feature-store"} == 0
    for: 30s
    labels:
      severity: critical
      team: data-platform
    annotations:
      summary: "Feature Store service is down"
      description: "Service has been down for more than 30 seconds"
- name: feature_store_warning
  rules:
  - alert: ModerateLatencyIncrease
    expr: histogram_quantile(0.95, rate(feature_store_request_duration_seconds_bucket[5m])) > 0.03
    for: 5m
    labels:
      severity: warning
      team: data-platform
    annotations:
      summary: "Feature Store latency increasing"
      description: "P95 latency is {{ $value }}s, approaching 40ms SLA"
      
  - alert: CacheHitRateDecreasing
    expr: feature_store_cache_hit_rate < 90
    for: 10m
    labels:
      severity: warning
      team: data-platform
    annotations:
      summary: "Cache performance degrading"
      description: "Cache hit rate dropped to {{ $value }}%"
      
  - alert: DatabaseConnectionsHigh
    expr: feature_store_db_connections{state="active"} > 80
    for: 5m
    labels:
      severity: warning
      team: data-platform
    annotations:
      summary: "High database connection usage"
      description: "Active connections: {{ $value }}/100"
      
  - alert: MemoryUsageHigh
    expr: (process_resident_memory_bytes / 1024 / 1024 / 1024) > 1.5
    for: 10m
    labels:
      severity: warning
      team: data-platform
    annotations:
      summary: "High memory usage"
      description: "Memory usage: {{ $value }}GB"

- name: feature_store_business
  rules:
  - alert: LowApprovalRate
    expr: bnpl_approval_rate < 60
    for: 15m
    labels:
      severity: warning
      team: risk-management
    annotations:
      summary: "Credit approval rate unusually low"
      description: "Approval rate: {{ $value }}% over last 15 minutes"
      
  - alert: HighRiskDecisions
    expr: rate(bnpl_credit_decisions_total{risk_level="high"}[15m]) / rate(bnpl_credit_decisions_total[15m]) > 0.3
    for: 10m
    labels:
      severity: warning
      team: risk-management
    annotations:
      summary: "High proportion of high-risk decisions"
      description: "{{ $value | humanizePercentage }} of decisions are high-risk"Alert Manager Configurationalertmanager.yml:global:
  smtp_smarthost: 'smtp.company.com:587'
  smtp_from: 'alerts@company.com'
  slack_api_url: 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'

route:
  group_by: ['alertname', 'severity']
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  receiver: 'default-receiver'
  
  routes:
  - match:
      severity: critical
    receiver: 'critical-alerts'
    group_wait: 10s
    repeat_interval: 1h
    
  - match:
      team: risk-management
    receiver: 'risk-team-alerts'
    
  - match:
      severity: warning
    receiver: 'warning-alerts'
    repeat_interval: 12h

receivers:
- name: 'default-receiver'
  slack_configs:
  - channel: '#data-platform'
    title: 'Feature Store Alert'
    text: '{{ range .Alerts }}{{ .Annotations.summary }}{{ end }}'

- name: 'critical-alerts'
  slack_configs:
  - channel: '#incidents'
    color: 'danger'
    title: '🚨 CRITICAL: Feature Store Issue'
    text: |
      {{ range .Alerts }}
      *Alert:* {{ .Annotations.summary }}
      *Description:* {{ .Annotations.description }}
      *Runbook:* {{ .Annotations.runbook_url }}
      {{ end }}
  email_configs:
  - to: 'oncall@company.com'
    subject: 'CRITICAL: Feature Store Alert'
    body: |
      {{ range .Alerts }}
      Alert: {{ .Annotations.summary }}
      Description: {{ .Annotations.description }}
      {{ end }}

- name: 'risk-team-alerts'
  slack_configs:
  - channel: '#risk-management'
    color: 'warning'
    title: '⚠️ Risk Management Alert'
    text: '{{ range .Alerts }}{{ .Annotations.summary }}: {{ .Annotations.description }}{{ end }}'Structured LoggingLog ConfigurationPython Logging Setup:import logging
import json
import time
from datetime import datetime
from typing import Dict, Any

class StructuredLogger:
    def __init__(self, service_name: str = "feature-store"):
        self.service_name = service_name
        self.logger = logging.getLogger(service_name)
        self.logger.setLevel(logging.INFO)
        
        # JSON formatter for structured logs
        handler = logging.StreamHandler()
        handler.setFormatter(self.JsonFormatter())
        self.logger.addHandler(handler)
    
    class JsonFormatter(logging.Formatter):
        def format(self, record):
            log_entry = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "level": record.levelname,
                "service": "feature-store",
                "message": record.getMessage(),
                "module": record.module,
                "function": record.funcName,
                "line": record.lineno
            }
            
            # Add extra fields if present
            if hasattr(record, 'user_id'):
                log_entry['user_id'] = record.user_id
            if hasattr(record, 'request_id'):
                log_entry['request_id'] = record.request_id
            if hasattr(record, 'duration_ms'):
                log_entry['duration_ms'] = record.duration_ms
            if hasattr(record, 'cache_hit'):
                log_entry['cache_hit'] = record.cache_hit
                
            return json.dumps(log_entry)
    
    def log_request(self, user_id: str, endpoint: str, duration_ms: float, cache_hit: bool, request_id: str = None):
        """Log API request with structured data"""
        self.logger.info(
            f"API request completed: {endpoint}",
            extra={
                'user_id': user_id,
                'endpoint': endpoint,
                'duration_ms': duration_ms,
                'cache_hit': cache_hit,
                'request_id': request_id or self.generate_request_id()
            }
        )
    
    def log_error(self, error: Exception, context: Dict[str, Any] = None):
        """Log error with context"""
        self.logger.error(
            f"Error occurred: {str(error)}",
            extra={
                'error_type': type(error).__name__,
                'error_message': str(error),
                'context': context or {}
            },
            exc_info=True
        )
    
    def log_business_event(self, event_type: str, user_id: str, data: Dict[str, Any]):
        """Log business events for analysis"""
        self.logger.info(
            f"Business event: {event_type}",
            extra={
                'event_type': event_type,
                'user_id': user_id,
                'event_data': data,
                'timestamp': time.time()
            }
        )

# Global logger instance
logger = StructuredLogger()

# Usage in API endpoints
@app.get("/api/v1/features/user/{user_id}")
async def get_user_features(user_id: str, request: Request):
    request_id = str(uuid.uuid4())
    start_time = time.time()
    
    try:
        features = await feature_service.get_user_features(user_id)
        duration_ms = (time.time() - start_time) * 1000
        
        # Log successful request
        logger.log_request(
            user_id=user_id,
            endpoint="/api/v1/features/user",
            duration_ms=duration_ms,
            cache_hit=features.get('cache_hit', False),
            request_id=request_id
        )
        
        # Log business event
        logger.log_business_event(
            event_type="feature_request",
            user_id=user_id,
            data={
                'risk_score': features.get('risk_features', {}).get('risk_score'),
                'total_orders': features.get('transaction_features', {}).get('total_orders')
            }
        )
        
        return features
        
    except Exception as e:
        logger.log_error(e, context={
            'user_id': user_id,
            'request_id': request_id,
            'endpoint': '/api/v1/features/user'
        })
        raiseLog Analysis QueriesCommon Log Analysis (using jq or similar):# Find slow requests (>100ms)
cat logs.json | jq 'select(.duration_ms > 100) | {timestamp, user_id, duration_ms, endpoint}'

# Cache hit rate analysis
cat logs.json | jq 'select(.cache_hit != null) | .cache_hit' | sort | uniq -c

# Error rate by endpoint
cat logs.json | jq 'select(.level == "ERROR") | .endpoint' | sort | uniq -c

# Top users by request volume
cat logs.json | jq '.user_id' | grep -v null | sort | uniq -c | sort -nr | head -10

# Business metrics from logs
cat logs.json | jq 'select(.event_type == "feature_request") | .event_data.risk_score' | \
  awk '{sum+=$1; count++} END {print "Average risk score:", sum/count}'Health ChecksService Health MonitoringComprehensive Health Check:from fastapi import FastAPI, Response, status
import asyncio
import time

class HealthChecker:
    def __init__(self):
        self.start_time = time.time()
        self.last_db_check = 0
        self.last_cache_check = 0
        self.check_interval = 30  # Check every 30 seconds
    
    async def check_database(self) -> Dict[str, Any]:
        """Check database connectivity and performance"""
        try:
            start_time = time.time()
            
            # Simple query to test connectivity
            result = await database.execute("SELECT 1 as test")
            
            db_latency = (time.time() - start_time) * 1000
            
            # Check connection pool status
            pool_status = {
                'total_connections': database.pool.get_size(),
                'used_connections': database.pool.get_size() - database.pool.get_available_size(),
                'available_connections': database.pool.get_available_size()
            }
            
            self.last_db_check = time.time()
            
            return {
                'status': 'healthy',
                'latency_ms': round(db_latency, 2),
                'pool_status': pool_status,
                'last_check': self.last_db_check
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'last_check': time.time()
            }
    
    async def check_cache(self) -> Dict[str, Any]:
        """Check Redis cache connectivity and performance"""
        try:
            start_time = time.time()
            
            # Test basic Redis operations
            test_key = f"health_check_{int(time.time())}"
            await redis_client.set(test_key, "test_value", ex=60)
            value = await redis_client.get(test_key)
            await redis_client.delete(test_key)
            
            cache_latency = (time.time() - start_time) * 1000
            
            # Get Redis info
            info = await redis_client.info()
            
            self.last_cache_check = time.time()
            
            return {
                'status': 'healthy',
                'latency_ms': round(cache_latency, 2),
                'memory_used_mb': round(info['used_memory'] / 1024 / 1024, 2),
                'connected_clients': info['connected_clients'],
                'last_check': self.last_cache_check
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'last_check': time.time()
            }
    
    async def check_bigquery(self) -> Dict[str, Any]:
        """Check BigQuery connectivity"""
        try:
            start_time = time.time()
            
            # Simple query to test connectivity
            query = "SELECT 1 as test"
            result = await bigquery_client.query(query)
            
            bq_latency = (time.time() - start_time) * 1000
            
            return {
                'status': 'healthy',
                'latency_ms': round(bq_latency, 2),
                'last_check': time.time()
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'last_check': time.time()
            }

health_checker = HealthChecker()

@app.get("/health")
async def health_check(response: Response):
    """Comprehensive health check endpoint"""
    
    # Run health checks in parallel
    db_health, cache_health, bq_health = await asyncio.gather(
        health_checker.check_database(),
        health_checker.check_cache(),
        health_checker.check_bigquery(),
        return_exceptions=True
    )
    
    uptime_seconds = time.time() - health_checker.start_time
    
    health_status = {
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'uptime_seconds': round(uptime_seconds, 2),
        'version': '1.0.0',
        'environment': os.getenv('ENV', 'local'),
        'components': {
            'database': db_health,
            'cache': cache_health,
            'bigquery': bq_health
        }
    }
    
    # Determine overall health
    component_statuses = [
        db_health.get('status', 'unhealthy'),
        cache_health.get('status', 'unhealthy'),
        bq_health.get('status', 'unhealthy')
    ]
    
    if 'unhealthy' in component_statuses:
        health_status['status'] = 'degraded' if 'healthy' in component_statuses else 'unhealthy'
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    
    return health_status

@app.get("/health/ready")
async def readiness_check():
    """Kubernetes readiness probe"""
    # Quick check without external dependencies
    return {
        'status': 'ready',
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }

@app.get("/health/live")
async def liveness_check():
    """Kubernetes liveness probe"""
    return {
        'status': 'alive',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'uptime_seconds': time.time() - health_checker.start_time
    }Performance AnalysisCustom Metrics DashboardPerformance Analysis Queries:# Request rate by endpoint
sum(rate(feature_store_requests_total[5m])) by (endpoint)

# Latency percentiles
histogram_quantile(0.50, rate(feature_store_request_duration_seconds_bucket[5m])) # P50
histogram_quantile(0.95, rate(feature_store_request_duration_seconds_bucket[5m])) # P95
histogram_quantile(0.99, rate(feature_store_request_duration_seconds_bucket[5m])) # P99

# Error rate
sum(rate(feature_store_requests_total{status=~"5.."}[5m])) / sum(rate(feature_store_requests_total[5m]))

# Cache performance breakdown
sum(rate(feature_store_requests_total{cache_hit="true"}[5m])) / sum(rate(feature_store_requests_total[5m])) * 100

# Database connection utilization
feature_store_db_connections{state="active"} / (feature_store_db_connections{state="active"} + feature_store_db_connections{state="idle"}) * 100

# Memory growth rate
deriv(process_resident_memory_bytes[1h])

# Business metrics
sum(rate(bnpl_credit_decisions_total{decision="approved"}[5m])) / sum(rate(bnpl_credit_decisions_total[5m])) * 100Troubleshooting RunbooksCommon Issues & SolutionsHigh Latency Runbook:# Runbook: High API Latency (P95 > 40ms)

## Investigation Steps
1. Check current metrics:
   - `histogram_quantile(0.95, rate(feature_store_request_duration_seconds_bucket[5m]))`
   - `feature_store_cache_hit_rate`
   - `feature_store_db_connections{state="active"}`

2. Identify bottleneck:
   - If cache hit rate < 90%: Check Redis health, memory usage
   - If DB connections > 80%: Check for slow queries, connection leaks
   - If memory usage > 80%: Check for memory leaks, consider restart

3. Immediate actions:
   - Scale up instances if CPU > 80%
   - Restart service if memory leak suspected
   - Clear cache if hit rate degraded

## Escalation
- If latency > 100ms for 5+ minutes: Page on-call engineer
- If no improvement in 15 minutes: Engage senior engineerThis comprehensive monitoring setup provides complete observability into the Feature Store's performance, health, and business metrics, enabling proactive issue detection and rapid incident response.