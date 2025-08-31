# BNPL Feature Store - API Reference

## Overview

The Feature Store provides both REST and gRPC APIs for flexible integration. REST APIs are optimized for web and mobile applications, while gRPC APIs provide high-performance access for internal microservices.

## REST API

### Base URL
- **Local**: `http://localhost:8000`
- **Production**: `https://feature-store-prod-{project}.a.run.app`

### Authentication
```bash
# API Key (recommended)
curl -H "X-API-Key: your_api_key" https://api.example.com/endpoint

# JWT Token (for user sessions)
curl -H "Authorization: Bearer jwt_token" https://api.example.com/endpointHealth CheckGET /healthCheck API service health.Response:{
  "status": "healthy",
  "timestamp": "2024-08-20T10:30:00Z",
  "version": "1.0.0",
  "components": {
    "database": "healthy",
    "cache": "healthy", 
    "bigquery": "healthy"
  }
}User FeaturesGET /api/v1/features/user/{user_id}Get comprehensive feature set for a user.Parameters:user_id (path): User identifierfeature_types (query): Comma-separated feature types [user,transaction,risk]Example:curl "http://localhost:8000/api/v1/features/user/user_000001?feature_types=user,risk"Response:{
  "user_id": "user_000001",
  "user_features": {
    "age": 28,
    "location_country": "AE",
    "signup_date": "2023-03-15",
    "verification_status": "verified",
    "income_bracket": "medium"
  },
  "transaction_features": {
    "total_orders": 25,
    "avg_order_value": 650.00,
    "last_order_days": 3,
    "favorite_category": "fashion",
    "monthly_spend": 1200.50
  },
  "risk_features": {
    "risk_score": 0.35,
    "fraud_probability": 0.12,
    "payment_delays": 1,
    "device_trust_score": 0.89,
    "credit_utilization": 0.45
  },
  "metadata": {
    "response_time_ms": 32.4,
    "cache_hit": true,
    "timestamp": "2024-08-20T10:30:00Z",
    "data_freshness_minutes": 15
  }
}Batch ProcessingPOST /api/v1/features/batchProcess multiple users efficiently.Request Body:{
  "requests": [
    {
      "user_id": "user_000001",
      "feature_types": ["user", "risk"]
    },
    {
      "user_id": "user_000002", 
      "feature_types": ["user", "transaction", "risk"]
    }
  ]
}Response:{
  "total_requests": 2,
  "successful_requests": 2,
  "failed_requests": 0,
  "total_response_time_ms": 45.7,
  "responses": [
    {
      "user_id": "user_000001",
      "user_features": { "..." },
      "risk_features": { "..." },
      "cache_hit": true
    },
    {
      "user_id": "user_000002",
      "user_features": { "..." },
      "transaction_features": { "..." },
      "risk_features": { "..." },
      "cache_hit": false
    }
  ]
}Error ResponsesStandard Error Format{
  "error": {
    "code": "USER_NOT_FOUND",
    "message": "User user_999999 not found",
    "details": {
      "user_id": "user_999999",
      "timestamp": "2024-08-20T10:30:00Z"
    }
  }
}Common Error Codes400 Bad Request: Invalid request format401 Unauthorized: Missing or invalid authentication404 Not Found: User not found429 Too Many Requests: Rate limit exceeded500 Internal Server Error: Server error503 Service Unavailable: Service temporarily unavailablegRPC APIService Definitionservice FeatureStore {
  rpc GetUserFeatures(UserFeatureRequest) returns (UserFeatureResponse);
  rpc GetBatchFeatures(BatchFeatureRequest) returns (BatchFeatureResponse);
}Connectionimport grpc
from proto import feature_store_pb2_grpc

channel = grpc.insecure_channel('localhost:50051')
stub = feature_store_pb2_grpc.FeatureStoreStub(channel)GetUserFeaturesRequest:from proto import feature_store_pb2

request = feature_store_pb2.UserFeatureRequest(
    user_id="user_000001",
    feature_types=["user", "transaction", "risk"]
)

response = stub.GetUserFeatures(request)Response Fields:response.user_id                    # "user_000001"
response.user_features.age          # 28
response.user_features.location_country # "AE"
response.transaction_features.total_orders # 25
response.risk_features.risk_score   # 0.35
response.cache_hit                  # True
response.timestamp                  # Unix timestampGetBatchFeaturesRequest:requests = [
    feature_store_pb2.UserFeatureRequest(
        user_id=f"user_{i:06d}",
        feature_types=["user", "risk"]
    )
    for i in range(1, 11)  # 10 users
]

batch_request = feature_store_pb2.BatchFeatureRequest(requests=requests)
response = stub.GetBatchFeatures(batch_request)Response:for user_response in response.responses:
    print(f"User: {user_response.user_id}")
    print(f"Risk Score: {user_response.risk_features.risk_score}")
    print(f"Cache Hit: {user_response.cache_hit}")Performance GuidelinesLatency ExpectationsAPI TypeTarget LatencyUse CaseREST API<40ms P95Web/mobile appsgRPC API<30ms P95Internal servicesBatch API<20ms per userBulk processingBest PracticesREST API Optimization# Request only needed feature types
curl "localhost:8000/api/v1/features/user/user_001?feature_types=risk"

# Use batch endpoint for multiple users
curl -X POST localhost:8000/api/v1/features/batch \
  -H "Content-Type: application/json" \
  -d '{"requests": [...]}'gRPC Optimization# Reuse connections
channel = grpc.insecure_channel('localhost:50051')
stub = feature_store_pb2_grpc.FeatureStoreStub(channel)

# Async for high concurrency
async def get_features_async():
    async with grpc.aio.insecure_channel('localhost:50051') as channel:
        stub = feature_store_pb2_grpc.FeatureStoreStub(channel)
        response = await stub.GetUserFeatures(request)

# Batch requests when possible
batch_request = feature_store_pb2.BatchFeatureRequest(requests=requests)Rate LimitingDefault: 1000 requests/minute per API keyBurst: Up to 100 requests in 10 secondsHeaders: X-RateLimit-Remaining, X-RateLimit-ResetCaching StrategyCache Hit Rate: 90%+ for optimal performanceTTL: 2 hours for user featuresCache Keys: feature:user:{user_id}:{feature_types}Integration ExamplesMobile App Integration (REST)import requests

def get_user_credit_decision(user_id: str, amount: float):
    response = requests.get(
        f"http://localhost:8000/api/v1/features/user/{user_id}",
        params={"feature_types": "user,risk"},
        timeout=10
    )
    
    if response.status_code == 200:
        features = response.json()
        risk_score = features["risk_features"]["risk_score"]
        
        # Simple approval logic
        if risk_score < 0.3 and amount < 500:
            return "APPROVED"
        else:
            return "DECLINED"
    
    return "ERROR"Internal Service Integration (gRPC)import grpc
from proto import feature_store_pb2, feature_store_pb2_grpc

class RiskEngine:
    def __init__(self):
        self.channel = grpc.insecure_channel('localhost:50051')
        self.stub = feature_store_pb2_grpc.FeatureStoreStub(self.channel)
    
    def calculate_risk_batch(self, user_ids: List[str]) -> Dict[str, float]:
        requests = [
            feature_store_pb2.UserFeatureRequest(
                user_id=user_id,
                feature_types=["risk"]
            )
            for user_id in user_ids
        ]
        
        batch_request = feature_store_pb2.BatchFeatureRequest(requests=requests)
        response = self.stub.GetBatchFeatures(batch_request)
        
        risk_scores = {}
        for user_response in response.responses:
            risk_scores[user_response.user_id] = user_response.risk_features.risk_score
        
        return risk_scoresAPI VersioningVersion StrategyURL Versioning: /api/v1/, /api/v2/Backward Compatibility: v1 supported for 12 months after v2 releaseDeprecation Notice: 6 months advance warningVersion DifferencesVersionFeaturesStatusv1Basic user/transaction/risk featuresCurrentv2ML-enhanced features, real-time updatesPlannedMonitoring & DebuggingRequest Tracing# Add trace header
curl -H "X-Trace-ID: trace-123" localhost:8000/api/v1/features/user/user_001Debug Mode# Enable detailed response metadata
curl "localhost:8000/api/v1/features/user/user_001?debug=true"Debug Response:{
  "user_id": "user_000001",
  "features": { "..." },
  "debug_info": {
    "cache_lookup_time_ms": 2.1,
    "database_query_time_ms": 0,
    "feature_computation_time_ms": 1.3,
    "cache_key": "feature:user:user_000001:user,transaction,risk",
    "data_source": "cache"
  }
}