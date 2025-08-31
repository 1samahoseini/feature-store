"""
Redis Cache Layer
High-performance feature caching with TTL and eviction
"""

import json
import time
from typing import Optional, Dict, Any, List

import redis.asyncio as redis
import structlog
from prometheus_client import Counter, Histogram

from src.config.settings import get_settings

logger = structlog.get_logger(__name__)

# Metrics
CACHE_OPERATIONS = Counter(
    'feature_store_cache_operations_total',
    'Cache operations',
    ['operation', 'result']
)

CACHE_LATENCY = Histogram(
    'feature_store_cache_latency_seconds',
    'Cache operation latency',
    ['operation']
)


class FeatureCache:
    """Redis-based feature cache"""

    def __init__(self):
        self.settings = get_settings()
        self.initialize_redis_client()
        self._connection_pool = None

    def initialize_redis_client(self):
        self.redis_client: Optional[redis.Redis] = None

    async def init(self):
        """Initialize Redis connection"""
        try:
            self._connection_pool = redis.ConnectionPool(
                host=self.settings.redis_host,
                port=self.settings.redis_port,
                db=self.settings.redis_db,
                password=self.settings.redis_password,
                decode_responses=True,
                max_connections=20,
                retry_on_timeout=True,
                socket_keepalive=True,
                socket_keepalive_options={},
                health_check_interval=30
            )

            self.redis_client = redis.Redis(
                connection_pool=self._connection_pool
            )

            # Test connection
            await self.redis_client.ping()
            logger.info("Redis cache initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Redis cache: {e}")
            raise

    async def close(self):
        """Close Redis connection"""
        if self.redis_client:
            await self.redis_client.close()
            logger.info("Redis cache connection closed")

    def _get_cache_key(self, user_id: str, feature_type: str) -> str:
        """Generate cache key"""
        return f"feature:{feature_type}:{user_id}"

    def _get_user_key(self, user_id: str) -> str:
        """Generate user-level cache key"""
        return f"user:{user_id}"

    async def get_features(self, user_id: str, feature_types: List[str]) -> Optional[Dict[str, Any]]:
        """Get cached features for user"""
        if not self.redis_client:
            return None

        start_time = time.time()

        try:
            cache_keys = [self._get_cache_key(user_id, ft) for ft in feature_types]

            async with self.redis_client.pipeline() as pipe:
                for key in cache_keys:
                    pipe.get(key)
                results = await pipe.execute()

            cached_features = {}
            cache_hit = False

            for i, result in enumerate(results):
                if result:
                    feature_type = feature_types[i]
                    try:
                        cached_features[feature_type] = json.loads(result)
                        cache_hit = True
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON in cache for key: {cache_keys[i]}")

            latency = time.time() - start_time
            CACHE_LATENCY.labels(operation='get').observe(latency)
            CACHE_OPERATIONS.labels(
                operation='get',
                result='hit' if cache_hit else 'miss'
            ).inc()

            if cache_hit:
                logger.debug(f"Cache hit for user {user_id}, features: {list(cached_features.keys())}")
                return cached_features
            else:
                logger.debug(f"Cache miss for user {user_id}")
                return None

        except Exception as e:
            logger.error(f"Cache get error: {e}")
            CACHE_OPERATIONS.labels(operation='get', result='error').inc()
            return None

    async def set_features(self, user_id: str, features: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Cache features for user"""
        if not self.redis_client:
            return False

        start_time = time.time()
        ttl = ttl or self.settings.cache_ttl

        try:
            async with self.redis_client.pipeline() as pipe:
                for feature_type, feature_data in features.items():
                    key = self._get_cache_key(user_id, feature_type)
                    value = json.dumps(feature_data, default=str)
                    pipe.setex(key, ttl, value)

                user_key = self._get_user_key(user_id)
                user_metadata = {
                    "cached_at": time.time(),
                    "feature_types": list(features.keys())
                }
                pipe.setex(user_key, ttl, json.dumps(user_metadata))

                await pipe.execute()

            latency = time.time() - start_time
            CACHE_LATENCY.labels(operation='set').observe(latency)
            CACHE_OPERATIONS.labels(operation='set', result='success').inc()

            logger.debug(f"Cached features for user {user_id}: {list(features.keys())}")
            return True

        except Exception as e:
            logger.error(f"Cache set error: {e}")
            CACHE_OPERATIONS.labels(operation='set', result='error').inc()
            return False

    async def delete_user_features(self, user_id: str) -> bool:
        """Delete all cached features for user"""
        if not self.redis_client:
            return False

        try:
            user_key = self._get_user_key(user_id)
            user_data = await self.redis_client.get(user_key)

            if user_data:
                metadata = json.loads(user_data)
                feature_types = metadata.get("feature_types", [])
            else:
                feature_types = ["user", "transaction", "risk"]

            async with self.redis_client.pipeline() as pipe:
                pipe.delete(user_key)
                for feature_type in feature_types:
                    key = self._get_cache_key(user_id, feature_type)
                    pipe.delete(key)
                await pipe.execute()

            CACHE_OPERATIONS.labels(operation='delete', result='success').inc()
            logger.debug(f"Deleted cache for user {user_id}")
            return True

        except Exception as e:
            logger.error(f"Cache delete error: {e}")
            CACHE_OPERATIONS.labels(operation='delete', result='error').inc()
            return False

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        if not self.redis_client:
            return {}

        try:
            info = await self.redis_client.info()

            return {
                "connected_clients": info.get("connected_clients", 0),
                "used_memory": info.get("used_memory_human", "0B"),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "expired_keys": info.get("expired_keys", 0),
                "evicted_keys": info.get("evicted_keys", 0)
            }
        except Exception as e:
            logger.error(f"Failed to get cache stats: {e}")
            return {}

    async def health_check(self) -> bool:
        """Check cache health"""
        if not self.redis_client:
            return False

        try:
            await self.redis_client.ping()
            return True
        except Exception:
            return False


# Global cache instance
cache = FeatureCache()