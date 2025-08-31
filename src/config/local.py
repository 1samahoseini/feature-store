"""
Local Development Configuration
"""


# Local-specific configurations
LOCAL_CONFIG = {
    "database": {
        "url": "postgresql://postgres:password@localhost:5432/feature_store",
        "pool_size": 10,
        "echo": True  # Enable SQL query logging in development
    },
    
    "redis": {
        "host": "localhost",
        "port": 6379,
        "decode_responses": True
    },
    
    "bigquery": {
        "emulator_host": "localhost:9050",
        "project": "local-project",
        "dataset": "feature_store"
    },
    
    "kafka": {
        "bootstrap_servers": ["localhost:9092"],
        "auto_offset_reset": "latest",
        "group_id": "feature-store-local"
    },
    
    "logging": {
        "level": "DEBUG",
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    }
}
