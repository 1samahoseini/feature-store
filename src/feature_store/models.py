"""
Pydantic Models for Feature Store
Data validation and serialization (Pydantic v2)
"""

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, ConfigDict


class FeatureType(str, Enum):
    """Feature type enumeration"""
    USER = "user"
    TRANSACTION = "transaction"
    RISK = "risk"


class UserFeatures(BaseModel):
    """User demographic and behavioral features"""
    user_id: str
    age: Optional[int] = Field(None, ge=18, le=100)
    location_country: Optional[str] = Field(None, max_length=2)
    location_city: Optional[str] = Field(None, max_length=100)
    total_orders: int = Field(default=0, ge=0)
    avg_order_value: float = Field(default=0.0, ge=0.0)
    days_since_first_order: Optional[int] = Field(None, ge=0)
    preferred_payment_method: Optional[str] = None
    account_verified: bool = Field(default=False)
    created_at: datetime
    updated_at: datetime


class TransactionFeatures(BaseModel):
    """Transaction-based features"""
    user_id: str
    total_transactions_30d: int = Field(default=0, ge=0)
    total_amount_30d: float = Field(default=0.0, ge=0.0)
    avg_transaction_amount: float = Field(default=0.0, ge=0.0)
    max_transaction_amount: float = Field(default=0.0, ge=0.0)
    transactions_declined_30d: int = Field(default=0, ge=0)
    unique_merchants_30d: int = Field(default=0, ge=0)
    weekend_transaction_ratio: float = Field(default=0.0, ge=0.0, le=1.0)
    night_transaction_ratio: float = Field(default=0.0, ge=0.0, le=1.0)
    created_at: datetime
    updated_at: datetime


class RiskFeatures(BaseModel):
    """Risk assessment features"""
    user_id: str
    credit_utilization_ratio: float = Field(default=0.0, ge=0.0, le=1.0)
    payment_delays_30d: int = Field(default=0, ge=0)
    payment_delays_90d: int = Field(default=0, ge=0)
    failed_payments_count: int = Field(default=0, ge=0)
    device_changes_30d: int = Field(default=0, ge=0)
    login_locations_30d: int = Field(default=0, ge=0)
    velocity_alerts_30d: int = Field(default=0, ge=0)
    risk_score: float = Field(default=0.0, ge=0.0, le=1.0)
    created_at: datetime
    updated_at: datetime


class FeatureRequest(BaseModel):
    """Feature request model"""
    user_id: str = Field(..., min_length=1, max_length=100)
    feature_types: List[FeatureType] = Field(default_factory=lambda: [FeatureType.USER])
    include_metadata: bool = Field(default=False)

    @field_validator('feature_types')
    @classmethod
    def validate_feature_types(cls, v: List[FeatureType]) -> List[FeatureType]:
        if not v:
            raise ValueError("At least one feature type is required")
        # Deduplicate (behavior consistent with original implementation)
        return list(set(v))


class BatchFeatureRequest(BaseModel):
    """Batch feature request model"""
    requests: List[FeatureRequest] = Field(...)

    @field_validator('requests')
    @classmethod
    def validate_requests_length(cls, v: List[FeatureRequest]) -> List[FeatureRequest]:
        if not (1 <= len(v) <= 100):
            raise ValueError("requests must contain between 1 and 100 items")
        return v


class FeatureResponse(BaseModel):
    """Feature response model"""
    user_id: str
    user_features: Optional[UserFeatures] = None
    transaction_features: Optional[TransactionFeatures] = None
    risk_features: Optional[RiskFeatures] = None
    response_time_ms: float
    cache_hit: bool
    data_freshness_minutes: Optional[int] = None

    model_config = ConfigDict(
        json_encoders={
            datetime: lambda dt: dt.isoformat()
        }
    )


class BatchFeatureResponse(BaseModel):
    """Batch feature response model"""
    responses: List[FeatureResponse]
    total_requests: int
    successful_requests: int
    failed_requests: int
    total_response_time_ms: float
    cache_hit_ratio: float


class HealthStatus(BaseModel):
    """Health status model"""
    status: str
    environment: str
    version: str
    timestamp: float
    checks: Dict[str, bool] = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    """Error response model"""
    error: str
    message: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    request_id: Optional[str] = None
