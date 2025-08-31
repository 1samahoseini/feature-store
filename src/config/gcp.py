"""
Google Cloud Platform Configuration
"""


# GCP-specific configurations
GCP_CONFIG = {
    "database": {
        "pool_size": 20,
        "pool_pre_ping": True,
        "pool_recycle": 3600,
        "echo": False  # Disable SQL logging in production
    },
    
    "redis": {
        "socket_keepalive": True,
        "socket_keepalive_options": {},
        "health_check_interval": 30
    },
    
    "bigquery": {
        "location": "US",
        "job_retry_max_attempts": 3,
        "query_timeout": 60  # seconds
    },
    
    "pubsub": {
        "ack_deadline": 60,  # seconds
        "max_messages": 1000,
        "max_latency": 0.1  # seconds
    },
    
    "cloud_run": {
        "cpu_utilization": 70,  # percentage
        "max_concurrent_requests": 100,
        "timeout": 300  # seconds
    },
    
    "logging": {
        "level": "INFO",
        "json_format": True,
        "stackdriver": True
    }
}
