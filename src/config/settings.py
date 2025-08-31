"""
Configuration Management
Environment-based settings with validation
"""

import os
from functools import lru_cache
from typing import Optional

from pydantic import Field, root_validator, ValidationError
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Base application settings with environment validation."""

    # Environment
    env: str = Field(default="local", description="Environment (local/gcp)")
    debug: bool = Field(default=True, description="Debug mode")

    # API Configuration
    host: str = Field(default="0.0.0.0", description="API host")
    port: int = Field(default=8000, description="API port")
    grpc_port: int = Field(default=50051, description="gRPC port")

    # Database
    database_url: str = Field(
        default="postgresql://postgres:password@localhost:5432/feature_store",
        description="Database URL"
    )
    database_pool_size: int = Field(default=20, description="DB pool size")
    database_max_overflow: int = Field(default=30, description="DB max overflow")

    # Redis
    redis_host: str = Field(default="localhost", description="Redis host")
    redis_port: int = Field(default=6379, description="Redis port")
    redis_db: int = Field(default=0, description="Redis database")
    redis_password: Optional[str] = Field(default=None, description="Redis password")

    # BigQuery
    bigquery_project_id: str = Field(default="local-project", description="BigQuery project")
    bigquery_dataset: str = Field(default="feature_store", description="BigQuery dataset")
    bigquery_location: str = Field(default="US", description="BigQuery location")

    # GCP
    gcp_project_id: Optional[str] = Field(default=None, description="GCP project ID")
    gcp_region: str = Field(default="us-central1", description="GCP region")
    google_application_credentials: Optional[str] = Field(
        default=None, description="GCP credentials path"
    )

    # Kafka / PubSub
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092", description="Kafka bootstrap servers"
    )
    kafka_topic_features: str = Field(
        default="feature-updates", description="Kafka features topic"
    )
    pubsub_topic_features: str = Field(
        default="feature-updates", description="Pub/Sub features topic"
    )

    # Performance
    cache_ttl: int = Field(default=3600, description="Cache TTL seconds")
    batch_size: int = Field(default=1000, description="Batch processing size")
    max_workers: int = Field(default=10, description="Max worker threads")

    # Monitoring
    prometheus_port: int = Field(default=9090, description="Prometheus port")
    stackdriver_enabled: bool = Field(default=False, description="Stackdriver enabled")

    class Config:
        env_file = ".env"
        case_sensitive = False


class LocalSettings(Settings):
    """Local development settings."""
    env: str = "local"
    debug: bool = True
    bigquery_emulator_host: str = "localhost:9050"


class GCPSettings(Settings):
    """GCP production settings with validation."""
    env: str = "gcp"
    debug: bool = False
    stackdriver_enabled: bool = True

    # Cloud Run configuration
    cloud_run_service: str = "feature-store"
    cloud_run_region: str = "us-central1"
    cloud_run_memory: str = "2Gi"
    cloud_run_cpu: int = 2
    cloud_run_min_instances: int = 2
    cloud_run_max_instances: int = 50

    @root_validator(skip_on_failure=True)
    def validate_gcp_fields(cls, values):
        """Ensure all critical GCP fields are set in production."""
        required_fields = ["gcp_project_id", "google_application_credentials", "bigquery_project_id"]
        missing = [f for f in required_fields if not values.get(f)]
        if missing:
            raise ValueError(f"GCP environment misconfigured, missing: {', '.join(missing)}")
        return values


@lru_cache()
def get_settings() -> Settings:
    """Return the appropriate settings based on ENV variable."""
    env = os.getenv("ENV", "local").lower()
    try:
        if env == "gcp":
            return GCPSettings()
        return LocalSettings()
    except ValidationError as e:
        # Fail fast if required GCP fields are missing
        raise RuntimeError(f"Settings validation failed: {e}")


# Example usage:
# settings = get_settings()
# print(settings.dict())
