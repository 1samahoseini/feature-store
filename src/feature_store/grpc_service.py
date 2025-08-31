"""
gRPC Service Implementation
High-performance binary protocol for feature serving
"""

import time

import grpc  # type: ignore
import structlog
from prometheus_client import Counter, Histogram

from src.proto import feature_store_pb2  # type: ignore
from src.proto import feature_store_pb2_grpc  # type: ignore
from src.feature_store.store import feature_store
from src.feature_store.models import FeatureRequest, BatchFeatureRequest
from src.feature_store.cache import cache
from src.feature_store.database import db  # type: ignore

logger = structlog.get_logger(__name__)

# gRPC Metrics
GRPC_REQUESTS = Counter(
    'feature_store_grpc_requests_total',
    'gRPC requests',
    ['method', 'status']
)

GRPC_LATENCY = Histogram(
    'feature_store_grpc_latency_seconds',
    'gRPC request latency',
    ['method']
)


class FeatureStoreServicer(feature_store_pb2_grpc.FeatureStoreServicer):
    """gRPC Feature Store Service Implementation"""

    async def GetUserFeatures(self, request, context):
        """Get features for a single user"""
        start_time = time.time()
        method_name = 'GetUserFeatures'

        try:
            # Validate request
            if not request.uid:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('uid is required')
                GRPC_REQUESTS.labels(method=method_name, status='invalid_argument').inc()
                return feature_store_pb2.UserFeatureResponse()  # type: ignore

            # Convert gRPC request to internal model
            feature_request = FeatureRequest(
                user_id=request.uid,
                feature_types=[
                    ft if isinstance(ft, FeatureRequest.FeatureType) else FeatureRequest.FeatureType[ft.upper()]
                    for ft in (list(request.feature_types) or ['user'])
                ],
                include_metadata=getattr(request, "include_metadata", True)
            )

            # Get features
            response = await feature_store.get_features(feature_request)

            # Convert to gRPC response
            grpc_response = feature_store_pb2.UserFeatureResponse(  # type: ignore
                uid=getattr(response, "uid", ""),
                demographics=self._convert_demographics(getattr(response, "demographics", {})),
                behavior=self._convert_behavior(getattr(response, "behavior", {})),
                risk=self._convert_risk(getattr(response, "risk", {})),
                response_time=getattr(response, "response_time", 0),
                cache_hit=getattr(response, "cache_hit", False),
                freshness_ms=getattr(response, "freshness_ms", 0),
                timestamp=getattr(response, "timestamp", int(time.time()))
            )

            # Record metrics
            latency = time.time() - start_time
            GRPC_LATENCY.labels(method=method_name).observe(latency)
            GRPC_REQUESTS.labels(method=method_name, status='success').inc()

            logger.info(
                "gRPC feature request processed",
                uid=request.uid,
                feature_types=list(request.feature_types),
                response_time_ms=getattr(response, "response_time", 0),
                cache_hit=getattr(response, "cache_hit", False),
                grpc_latency_ms=latency * 1000
            )

            return grpc_response

        except Exception as e:
            logger.error(f"gRPC GetUserFeatures error: {e}", uid=getattr(request, "uid", None))
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details('Internal server error')
            GRPC_REQUESTS.labels(method=method_name, status='error').inc()
            return feature_store_pb2.UserFeatureResponse()  # type: ignore

    async def GetBatchFeatures(self, request, context):
        """Get features for multiple users"""
        start_time = time.time()
        method_name = 'GetBatchFeatures'

        try:
            # Validate request
            if not getattr(request, "requests", []):
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('requests list cannot be empty')
                GRPC_REQUESTS.labels(method=method_name, status='invalid_argument').inc()
                return feature_store_pb2.BatchFeatureResponse()  # type: ignore

            if len(request.requests) > 100:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('batch size too large: maximum 100 requests')
                GRPC_REQUESTS.labels(method=method_name, status='invalid_argument').inc()
                return feature_store_pb2.BatchFeatureResponse()  # type: ignore

            # Convert gRPC requests to internal models
            feature_requests = []
            for req in request.requests:
                if not getattr(req, "uid", None):
                    continue
                feature_requests.append(FeatureRequest(
                    user_id=req.uid,
                    feature_types=[ft if isinstance(ft, FeatureRequest.FeatureType) else FeatureRequest.FeatureType[ft.upper()] for ft in (getattr(req, "feature_types", []) or ['user'])],
                    include_metadata=getattr(req, "include_metadata", True)
                ))

            if not feature_requests:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('no valid requests found')
                GRPC_REQUESTS.labels(method=method_name, status='invalid_argument').inc()
                return feature_store_pb2.BatchFeatureResponse()  # type: ignore

            # Process batch request
            batch_request = BatchFeatureRequest(requests=feature_requests)
            response = await feature_store.get_batch_features(batch_request)

            # Convert to gRPC response
            grpc_response = feature_store_pb2.BatchFeatureResponse(  # type: ignore
                total_requests=getattr(response, "total_requests", 0),
                successful_requests=getattr(response, "successful_requests", 0),
                failed_requests=getattr(response, "failed_requests", 0),
                total_response_time_ms=getattr(response, "total_response_time_ms", 0),
                cache_hit_ratio=getattr(response, "cache_hit_ratio", 0.0)
            )

            # Add individual responses
            for resp in getattr(response, "responses", []):
                user_response = feature_store_pb2.UserFeatureResponse(  # type: ignore
                    uid=getattr(resp, "uid", ""),
                    demographics=self._convert_demographics(getattr(resp, "demographics", {})),
                    behavior=self._convert_behavior(getattr(resp, "behavior", {})),
                    risk=self._convert_risk(getattr(resp, "risk", {})),
                    response_time=getattr(resp, "response_time", 0),
                    cache_hit=getattr(resp, "cache_hit", False),
                    freshness_ms=getattr(resp, "freshness_ms", 0),
                    timestamp=getattr(resp, "timestamp", int(time.time()))
                )
                grpc_response.responses.append(user_response)

            # Record metrics
            latency = time.time() - start_time
            GRPC_LATENCY.labels(method=method_name).observe(latency)
            GRPC_REQUESTS.labels(method=method_name, status='success').inc()

            logger.info(
                "gRPC batch feature request processed",
                total_requests=getattr(response, "total_requests", 0),
                successful_requests=getattr(response, "successful_requests", 0),
                cache_hit_ratio=getattr(response, "cache_hit_ratio", 0.0),
                grpc_latency_ms=latency * 1000
            )

            return grpc_response

        except Exception as e:
            logger.error(f"gRPC GetBatchFeatures error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details('Internal server error')
            GRPC_REQUESTS.labels(method=method_name, status='error').inc()
            return feature_store_pb2.BatchFeatureResponse()  # type: ignore

    async def HealthCheck(self, request, context):
        """Health check endpoint"""
        try:
            cache_healthy = await cache.health_check()
            db_healthy = await db.health_check()

            overall_healthy = cache_healthy and db_healthy

            response = feature_store_pb2.HealthCheckResponse(  # type: ignore
                status="healthy" if overall_healthy else "unhealthy",
                timestamp=int(time.time()),
                version="1.0.0",
                uptime=int(time.time())
            )

            GRPC_REQUESTS.labels(method='HealthCheck', status='success').inc()
            return response

        except Exception as e:
            logger.error(f"gRPC health check error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details('Health check failed')
            GRPC_REQUESTS.labels(method='HealthCheck', status='error').inc()
            return feature_store_pb2.HealthCheckResponse(  # type: ignore
                status="error",
                timestamp=int(time.time()),
                version="1.0.0",
                uptime=0
            )

    def _convert_demographics(self, demo):
        return feature_store_pb2.DemographicFeatures(  # type: ignore
            age=getattr(demo, "age", 0),
            location=getattr(demo, "location", ""),
            income_est=getattr(demo, "income_est", 0.0),
            segment=getattr(demo, "segment", "")
        )

    def _convert_behavior(self, behavior):
        return feature_store_pb2.BehaviorFeatures(  # type: ignore
            total_orders=getattr(behavior, "total_orders", 0),
            avg_order_value=getattr(behavior, "avg_order_value", 0.0),
            days_since_reg=getattr(behavior, "days_since_reg", 0),
            fav_category=getattr(behavior, "fav_category", ""),
            session_duration=getattr(behavior, "session_duration", 0)
        )

    def _convert_risk(self, risk):
        return feature_store_pb2.RiskFeatures(  # type: ignore
            payment_delays=getattr(risk, "payment_delays", 0),
            utilization_rate=getattr(risk, "utilization_rate", 0.0),
            credit_age=getattr(risk, "credit_age", 0),
            default_probability=getattr(risk, "default_probability", 0.0)
        )
