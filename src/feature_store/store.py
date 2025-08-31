"""
Feature Store Orchestration
Coordinates cache and database operations
"""

import time
from datetime import datetime
from typing import Dict, Any, List, Optional, Sequence

import structlog
from prometheus_client import Counter, Histogram

from src.feature_store.cache import cache
from src.feature_store.database import db
from src.feature_store.models import (
    FeatureRequest, FeatureResponse, BatchFeatureRequest, BatchFeatureResponse,
    UserFeatures, TransactionFeatures, RiskFeatures
)

logger = structlog.get_logger(__name__)

# Metrics
FEATURE_REQUESTS = Counter(
    'feature_store_requests_total',
    'Feature requests',
    ['feature_type', 'cache_result']
)

FEATURE_LATENCY = Histogram(
    'feature_store_feature_latency_seconds',
    'Feature request latency',
    ['feature_type', 'path']
)


class FeatureStore:
    """Main feature store orchestration class"""
    
    def __init__(self):
        self.cache = cache
        self.database = db

    async def get_features(self, request: FeatureRequest) -> FeatureResponse:
        """Get features for a single user"""
        start_time = time.time()
        feature_types: List[str] = [str(ft) for ft in request.feature_types]
        cached_features = await self._get_cached_features(request.user_id, feature_types)

        cache_hit = cached_features is not None and self._has_all_features(cached_features, feature_types)
        if cache_hit:
            response_time = (time.time() - start_time) * 1000
            self._record_metrics(feature_types, 'hit', response_time)
            return FeatureResponse(
                user_id=request.user_id,
                user_features=self._parse_features(cached_features.get('user') if cached_features else None, UserFeatures),
                transaction_features=self._parse_features(cached_features.get('transaction') if cached_features else None, TransactionFeatures),
                risk_features=self._parse_features(cached_features.get('risk') if cached_features else None, RiskFeatures),
                response_time_ms=response_time,
                cache_hit=True,
                data_freshness_minutes=self._calculate_freshness(cached_features) if cached_features else None
            )

        # Cache miss: fetch from DB
        db_features = await self._get_database_features(request.user_id, feature_types)
        if db_features:
            await self._cache_features(request.user_id, db_features)

        response_time = (time.time() - start_time) * 1000
        self._record_metrics(feature_types, 'miss', response_time)
        return FeatureResponse(
            user_id=request.user_id,
            user_features=self._parse_features(db_features.get('user'), UserFeatures),
            transaction_features=self._parse_features(db_features.get('transaction'), TransactionFeatures),
            risk_features=self._parse_features(db_features.get('risk'), RiskFeatures),
            response_time_ms=response_time,
            cache_hit=False,
            data_freshness_minutes=0
        )

    async def get_batch_features(self, request: BatchFeatureRequest) -> BatchFeatureResponse:
        """Get features for multiple users efficiently"""
        start_time = time.time()

        responses: List[FeatureResponse] = []
        cache_hits, failed_requests = 0, 0
        all_feature_types: List[str] = list({str(ft) for r in request.requests for ft in r.feature_types})

        cached_results: Dict[str, Dict[str, Any]] = {}
        cache_miss_users: List[str] = []

        for req in request.requests:
            cached = await self._get_cached_features(req.user_id, list(str(ft) for ft in req.feature_types))
            if cached and self._has_all_features(cached, list(str(ft) for ft in req.feature_types)):
                cached_results[req.user_id] = cached
                cache_hits += 1
            else:
                cache_miss_users.append(req.user_id)

        db_results: Dict[str, Dict[str, Any]] = {}
        if cache_miss_users:
            raw_db_results = await self.database.get_batch_features(cache_miss_users, all_feature_types)
            # Convert list to dict if necessary
            if isinstance(raw_db_results, list):
                # Assume each dict in the list has a 'user_id' key
                db_results = {item['user_id']: item for item in raw_db_results if 'user_id' in item}
            else:
                db_results = raw_db_results
            for user_id, features in db_results.items():
                if features:
                    await self._cache_features(user_id, features)

        for req in request.requests:
            try:
                features = cached_results.get(req.user_id) or db_results.get(req.user_id) or {}
                cache_hit = req.user_id in cached_results
                freshness = self._calculate_freshness(features) if cache_hit else 0

                responses.append(FeatureResponse(
                    user_id=req.user_id,
                    user_features=self._parse_features(features.get('user'), UserFeatures),
                    transaction_features=self._parse_features(features.get('transaction'), TransactionFeatures),
                    risk_features=self._parse_features(features.get('risk'), RiskFeatures),
                    response_time_ms=0,
                    cache_hit=cache_hit,
                    data_freshness_minutes=freshness
                ))
            except Exception as e:
                logger.error("Batch feature processing error", user_id=req.user_id, error=str(e))
                failed_requests += 1

        total_response_time = (time.time() - start_time) * 1000
        cache_hit_ratio = cache_hits / len(request.requests) if request.requests else 0.0

        return BatchFeatureResponse(
            responses=responses,
            total_requests=len(request.requests),
            successful_requests=len(responses),
            failed_requests=failed_requests,
            total_response_time_ms=total_response_time,
            cache_hit_ratio=cache_hit_ratio
        )

    async def _get_cached_features(self, user_id: str, feature_types: Sequence[str]) -> Optional[Dict[str, Any]]:
        try:
            return await self.cache.get_features(user_id, list(feature_types))
        except Exception as e:
            logger.error("Cache access error", user_id=user_id, error=str(e))
            return None

    async def _get_database_features(self, user_id: str, feature_types: Sequence[str]) -> Dict[str, Any]:
        features: Dict[str, Any] = {}
        try:
            if 'user' in feature_types:
                user = await self.database.get_user_features(user_id)
                if user: features['user'] = user.dict()  # noqa: E701
            if 'transaction' in feature_types:
                txn = await self.database.get_transaction_features(user_id)
                if txn: features['transaction'] = txn.dict()  # noqa: E701
            if 'risk' in feature_types:
                risk = await self.database.get_risk_features(user_id)
                if risk: features['risk'] = risk.dict()  # noqa: E701
            self._record_metrics(list(feature_types), 'miss')
            return features
        except Exception as e:
            logger.error("Database access error", user_id=user_id, error=str(e))
            return {}

    async def _cache_features(self, user_id: str, features: Dict[str, Any]):
        try:
            await self.cache.set_features(user_id, features)
        except Exception as e:
            logger.error("Cache set error", user_id=user_id, error=str(e))

    def _has_all_features(self, cached: Dict[str, Any], requested: Sequence[str]) -> bool:
        return all(ft in cached for ft in requested)

    def _parse_features(self, data: Optional[Dict[str, Any]], model_class):
        if not data:
            return None
        try:
            for key in ['created_at', 'updated_at']:
                if isinstance(data.get(key), str):
                    data[key] = datetime.fromisoformat(data[key].replace('Z', '+00:00'))
            return model_class(**data)
        except Exception as e:
            logger.error("Feature parsing error", model=model_class.__name__, error=str(e))
            return None

    def _calculate_freshness(self, features: Dict[str, Any]) -> Optional[int]:
        try:
            timestamps = [
                datetime.fromisoformat(f['updated_at'].replace('Z', '+00:00'))
                for f in features.values() if f and 'updated_at' in f
            ]
            if not timestamps:
                return None
            oldest = min(timestamps)
            return int((datetime.utcnow() - oldest.replace(tzinfo=None)).total_seconds() / 60)
        except Exception as e:
            logger.error("Freshness calculation error", error=str(e))
            return None

    def _record_metrics(self, feature_types: Sequence[str], cache_result: str, latency_ms: Optional[float] = None):
        for ft in feature_types:
            FEATURE_REQUESTS.labels(feature_type=ft, cache_result=cache_result).inc()
            if latency_ms is not None:
                FEATURE_LATENCY.labels(feature_type=ft, path='get_features').observe(latency_ms / 1000)


# Global feature store instance
feature_store = FeatureStore()
