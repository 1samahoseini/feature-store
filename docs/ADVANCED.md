# BNPL Feature Store - Advanced Topics

## Overview

Advanced architectural patterns, optimization techniques, and scaling strategies for the BNPL Feature Store. This document covers complex scenarios, performance tuning, and future architectural evolution.

## Advanced Caching Strategies

### Multi-Tier Cache Hierarchy

**Intelligent Cache Routing:**
```python
from enum import Enum
from typing import Optional, Dict, Any
import asyncio
import hashlib

class CacheStrategy(Enum):
    HOT_ONLY = "hot"           # Frequently accessed users
    WARM_PREFERRED = "warm"    # Occasionally accessed users  
    COLD_ACCEPTABLE = "cold"   # Rarely accessed users
    BYPASS_CACHE = "bypass"    # Real-time requirements

class AdaptiveCacheManager:
    def __init__(self):
        # Cache tiers with different characteristics
        self.l1_memory = {}        # In-process: <1ms, 1K items
        self.l2_redis_hot = None   # Redis SSD: <5ms, 100K items
        self.l3_redis_warm = None  # Redis HDD: <15ms, 1M items
        self.l4_database = None    # PostgreSQL: <50ms, unlimited
        
        # Access pattern tracking
        self.access_patterns = {}
        self.promotion_threshold = 10  # Promote after 10 accesses
        
    async def get_user_features(self, user_id: str, strategy: CacheStrategy = None) -> Dict[str, Any]:
        """Intelligent cache retrieval based on access patterns"""
        
        # Auto-detect strategy if not specified
        if strategy is None:
            strategy = self._determine_cache_strategy(user_id)
        
        # Update access patterns
        self._track_access(user_id)
        
        # Route based on strategy
        if strategy == CacheStrategy.HOT_ONLY:
            return await self._get_from_hot_path(user_id)
        elif strategy == CacheStrategy.WARM_PREFERRED:
            return await self._get_with_warm_fallback(user_id)
        elif strategy == CacheStrategy.BYPASS_CACHE:
            return await self._get_from_database(user_id)
        else:
            return await self._get_adaptive(user_id)
    
    def _determine_cache_strategy(self, user_id: str) -> CacheStrategy:
        """Determine optimal caching strategy based on access patterns"""
        access_count = self.access_patterns.get(user_id, 0)
        
        # Hash user_id to determine tier affinity
        hash_val = int(hashlib.md5(user_id.encode()).hexdigest()[:8], 16)
        tier_affinity = hash_val % 100
        
        if access_count > 100:  # Very frequent access
            return CacheStrategy.HOT_ONLY
        elif access_count > 10 or tier_affinity < 20:  # Frequent access or 20% hot tier
            return CacheStrategy.WARM_PREFERRED
        elif tier_affinity < 60:  # 40% warm tier
            return CacheStrategy.WARM_PREFERRED
        else:  # 40% cold tier
            return CacheStrategy.COLD_ACCEPTABLE
    
    async def _get_adaptive(self, user_id: str) -> Dict[str, Any]:
        """Multi-tier retrieval with automatic promotion"""
        
        # L1: In-memory cache (fastest)
        if user_id in self.l1_memory:
            self._track_cache_hit('l1')
            return self.l1_memory[user_id]
        
        # L2: Redis hot cache
        hot_data = await self.l2_redis_hot.get(f"hot:{user_id}")
        if hot_data:
            features = json.loads(hot_data)
            # Promote to L1 if frequently accessed
            if self.access_patterns.get(user_id, 0) > self.promotion_threshold:
                self.l1_memory[user_id] = features
            self._track_cache_hit('l2')
            return features
        
        # L3: Redis warm cache
        warm_data = await self.l3_redis_warm.get(f"warm:{user_id}")
        if warm_data:
            features = json.loads(warm_data)
            # Promote to hot cache
            await self.l2_redis_hot.set(f"hot:{user_id}", json.dumps(features), ex=3600)
            self._track_cache_hit('l3')
            return features
        
        # L4: Database (populate all cache levels)
        features = await self._get_from_database(user_id)
        await self._populate_caches(user_id, features)
        self._track_cache_hit('database')
        return features
    
    async def _populate_caches(self, user_id: str, features: Dict[str, Any]):
        """Intelligently populate cache tiers"""
        serialized = json.dumps(features)
        
        # Always populate warm cache
        await self.l3_redis_warm.set(f"warm:{user_id}", serialized, ex=7200)
        
        # Selectively populate hot cache based on predicted access
        if self._predict_hot_access(user_id, features):
            await self.l2_redis_hot.set(f"hot:{user_id}", serialized, ex=3600)
    
    def _predict_hot_access(self, user_id: str, features: Dict[str, Any]) -> bool:
        """Predict if user will be frequently accessed"""
        # High-risk users are accessed more frequently
        risk_score = features.get('risk_features', {}).get('risk_score', 0.5)
        
        # Recent transactions indicate activity
        last_order_days = features.get('transaction_features', {}).get('last_order_days', 999)
        
        # Predict based on risk and recency
        return risk_score > 0.7 or last_order_days < 7Cache Coherence & ConsistencyEvent-Driven Cache Invalidation:import asyncio
from dataclasses import dataclass
from typing import Set, List
import redis.asyncio as redis

@dataclass
class InvalidationEvent:
    user_id: str
    event_type: str  # 'transaction', 'profile_update', 'payment_delay'
    timestamp: float
    affected_features: Set[str]

class CacheCoherenceManager:
    def __init__(self):
        self.redis_cluster = redis.RedisCluster(
            startup_nodes=[
                {"host": "redis-node-1", "port": 7000},
                {"host": "redis-node-2", "port": 7000},
                {"host": "redis-node-3", "port": 7000},
            ],
            decode_responses=True
        )
        self.invalidation_queue = asyncio.Queue()
        
    async def handle_user_event(self, event: InvalidationEvent):
        """Handle user events that require cache invalidation"""
        
        # Determine cache keys to invalidate
        cache_keys = self._get_affected_cache_keys(event)
        
        # Choose invalidation strategy based on event type
        if event.event_type == 'transaction':
            await self._incremental_update(event, cache_keys)
        elif event.event_type == 'profile_update':
            await self._selective_invalidation(event, cache_keys)
        else:
            await self._full_invalidation(cache_keys)
    
    async def _incremental_update(self, event: InvalidationEvent, cache_keys: List[str]):
        """Update cache incrementally instead of full invalidation"""
        
        # For transaction events, we can update counters without full recalculation
        user_id = event.user_id
        
        # Update transaction counters
        transaction_key = f"features:user:{user_id}:transactions"
        
        # Use Redis HINCRBY for atomic counter updates
        pipeline = self.redis_cluster.pipeline()
        pipeline.hincrby(transaction_key, 'total_orders', 1)
        pipeline.hincrbyfloat(transaction_key, 'total_spent', event.amount)
        pipeline.hset(transaction_key, 'last_order_date', event.timestamp)
        
        await pipeline.execute()
        
        # Recalculate derived features asynchronously
        asyncio.create_task(self._recalculate_derived_features(user_id))
    
    async def _write_through_cache_update(self, user_id: str, updates: Dict[str, Any]):
        """Write-through cache update pattern"""
        
        # 1. Update database first (consistency)
        await self.database.update_user_features(user_id, updates)
        
        # 2. Update all cache layers
        cache_keys = [
            f"hot:user:{user_id}",
            f"warm:user:{user_id}",
            f"features:user:{user_id}"
        ]
        
        # Get current cached data
        current_features = await self.get_cached_features(user_id)
        if current_features:
            # Merge updates
            updated_features = {**current_features, **updates}
            serialized = json.dumps(updated_features)
            
            # Update all cache levels atomically
            pipeline = self.redis_cluster.pipeline()
            for key in cache_keys:
                pipeline.set(key, serialized)
            await pipeline.execute()
    
    async def _distributed_cache_invalidation(self, user_ids: List[str]):
        """Invalidate cache across multiple Redis nodes"""
        
        # Group user_ids by Redis node for efficient invalidation
        node_groups = self._group_by_redis_node(user_ids)
        
        # Parallel invalidation across nodes
        tasks = []
        for node, user_group in node_groups.items():
            task = self._invalidate_on_node(node, user_group)
            tasks.append(task)
        
        await asyncio.gather(*tasks)Advanced Database PatternsRead/Write Splitting with Consistency GuaranteesIntelligent Query Routing:from enum import Enum
import time
import asyncio

class ConsistencyLevel(Enum):
    EVENTUAL = "eventual"      # Read from replicas, eventual consistency
    STRONG = "strong"          # Read from primary, strong consistency
    CAUSAL = "causal"          # Read-after-write consistency

class DatabaseRouter:
    def __init__(self):
        self.primary = create_connection("postgresql://primary/feature_store")
        self.replicas = [
            create_connection("postgresql://replica-1/feature_store"),
            create_connection("postgresql://replica-2/feature_store"),
            create_connection("postgresql://replica-3/feature_store")
        ]
        self.write_timestamps = {}  # Track recent writes per user
        self.replication_lag_ms = 50  # Estimated replication lag
        
    async def execute_query(self, 
                          query: str, 
                          params: tuple, 
                          consistency: ConsistencyLevel = ConsistencyLevel.EVENTUAL,
                          user_id: str = None) -> Any:
        """Route queries based on consistency requirements"""
        
        if self._is_write_query(query):
            # All writes go to primary
            result = await self.primary.fetch(query, *params)
            if user_id:
                self.write_timestamps[user_id] = time.time()
            return result
        
        # Read routing based on consistency level
        if consistency == ConsistencyLevel.STRONG:
            return await self.primary.fetch(query, *params)
        
        elif consistency == ConsistencyLevel.CAUSAL and user_id:
            # Read-after-write consistency
            last_write = self.write_timestamps.get(user_id, 0)
            if time.time() - last_write < (self.replication_lag_ms / 1000):
                # Recent write, use primary
                return await self.primary.fetch(query, *params)
        
        # Use replica for eventual consistency
        replica = self._select_replica()
        return await replica.fetch(query, *params)
    
    def _select_replica(self):
        """Intelligent replica selection based on health and load"""
        # Simple round-robin for now, could be enhanced with:
        # - Health checking
        # - Load balancing
        # - Geographic proximity
        return self.replicas[int(time.time()) % len(self.replicas)]

# Usage in feature service
class AdvancedFeatureService:
    def __init__(self):
        self.db_router = DatabaseRouter()
        
    async def get_user_features_eventually_consistent(self, user_id: str):
        """Fast read from replica, eventual consistency acceptable"""
        query = """
        SELECT user_features, transaction_features, risk_features
        FROM user_feature_view 
        WHERE user_id = $1
        """
        
        return await self.db_router.execute_query(
            query, (user_id,),
            consistency=ConsistencyLevel.EVENTUAL,
            user_id=user_id
        )
    
    async def get_user_features_after_write(self, user_id: str):
        """Read after recent write, strong consistency required"""
        query = """
        SELECT user_features, transaction_features, risk_features
        FROM user_feature_view 
        WHERE user_id = $1
        """
        
        return await self.db_router.execute_query(
            query, (user_id,),
            consistency=ConsistencyLevel.CAUSAL,
            user_id=user_id
        )Advanced Query OptimizationDynamic Query Generation:
class QueryOptimizer:
    def __init__(self):
        self.query_cache = {}
        self.query_stats = {}  # Track query performance
        self.feature_selectivity = {
            'user_features': 0.9,      # High selectivity - always needed
            'transaction_features': 0.7, # Medium selectivity
            'risk_features': 0.8       # High selectivity for credit decisions
        }
        
    def build_optimized_query(self, 
                            user_ids: List[str], 
                            requested_features: List[str],
                            context: str = "api_request") -> str:
        """Build optimized query based on request patterns and selectivity"""
        
        # Create cache key for query plan
        cache_key = f"{len(user_ids)}:{':'.join(sorted(requested_features))}:{context}"
        
        if cache_key in self.query_cache:
            return self.query_cache[cache_key]
        
        # Build query with optimal joins based on feature selectivity
        base_query = "SELECT u.user_id"
        joins = ["FROM users u"]
        conditions = ["WHERE u.user_id = ANY($1::text[])"]
        
        if 'user' in requested_features:
            base_query += """, 
                jsonb_build_object(
                    'age', u.age,
                    'location_country', u.location_country,
                    'verification_status', u.verification_status,
                    'days_since_signup', EXTRACT(DAYS FROM (CURRENT_DATE - u.signup_date))
                ) as user_features"""
        
        if 'transaction' in requested_features:
            # Use materialized view for better performance
            if len(user_ids) > 100:
                joins.append("LEFT JOIN user_transaction_features_mv tf ON u.user_id = tf.user_id")
            else:
                # Dynamic aggregation for small batches
                joins.append("""LEFT JOIN (
                    SELECT 
                        user_id,
                        jsonb_build_object(
                            'total_orders', COUNT(*),
                            'avg_order_value', AVG(amount),
                            'last_order_days', EXTRACT(DAYS FROM (CURRENT_TIMESTAMP - MAX(created_at))),
                            'favorite_category', MODE() WITHIN GROUP (ORDER BY category)
                        ) as transaction_features
                    FROM transactions t
                    WHERE t.user_id = ANY($1::text[])
                      AND t.status = 'completed'
                      AND t.created_at >= CURRENT_DATE - INTERVAL '2 years'
                    GROUP BY user_id
                ) tf ON u.user_id = tf.user_id""")
            
            base_query += ", COALESCE(tf.transaction_features, '{}'::jsonb) as transaction_features"
        
        if 'risk' in requested_features:
            joins.append("LEFT JOIN risk_scores rs ON u.user_id = rs.user_id")
            conditions.append("AND (rs.calculated_at IS NULL OR rs.calculated_at >= CURRENT_DATE - INTERVAL '1 day')")
            base_query += """, 
                jsonb_build_object(
                    'risk_score', COALESCE(rs.risk_score, 0.5),
                    'fraud_probability', COALESCE(rs.fraud_probability, 0.1),
                    'payment_delays', COALESCE(rs.payment_delays, 0),
                    'device_trust_score', COALESCE(rs.device_trust_score, 0.5)
                ) as risk_features"""
        
        # Combine query parts
        optimized_query = f"{base_query}\n{' '.join(joins)}\n{' '.join(conditions)}"
        
        # Add performance hints for large batches
        if len(user_ids) > 1000:
            optimized_query = f"/*+ USE_NL(u tf rs) */ {optimized_query}"
        
        # Cache the optimized query
        self.query_cache[cache_key] = optimized_query
        return optimized_query
    
    async def execute_with_monitoring(self, query: str, params: tuple) -> List[Dict]:
        """Execute query with performance monitoring"""
        start_time = time.time()
        
        try:
            result = await self.database.fetch(query, *params)
            execution_time = (time.time() - start_time) * 1000
            
            # Track query performance
            query_hash = hashlib.md5(query.encode()).hexdigest()[:8]
            if query_hash not in self.query_stats:
                self.query_stats[query_hash] = {
                    'execution_times': [],
                    'row_counts': [],
                    'error_count': 0
                }
            
            self.query_stats[query_hash]['execution_times'].append(execution_time)
            self.query_stats[query_hash]['row_counts'].append(len(result))
            
            # Log slow queries
            if execution_time > 100:  # > 100ms
                logger.warning(f"Slow query detected: {execution_time:.1f}ms", extra={
                    'query_hash': query_hash,
                    'execution_time_ms': execution_time,
                    'row_count': len(result),
                    'query_preview': query[:100] + "..." if len(query) > 100 else query
                })
            
            return result
            
        except Exception as e:
            query_hash = hashlib.md5(query.encode()).hexdigest()[:8]
            if query_hash in self.query_stats:
                self.query_stats[query_hash]['error_count'] += 1
            raiseHigh-Availability PatternsCircuit Breaker with Intelligent FallbacksAdvanced Circuit Breaker Implementation:from enum import Enum
import asyncio
import time
from typing import Callable, Any, Optional

class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, all requests blocked
    HALF_OPEN = "half_open" # Testing recovery

class CircuitBreakerConfig:
    def __init__(self):
        self.failure_threshold = 5      # Open after 5 failures
        self.recovery_timeout = 30      # Try recovery after 30s
        self.success_threshold = 3      # Close after 3 successes
        self.timeout_seconds = 5        # Request timeout
        self.expected_exception_types = (ConnectionError, TimeoutError, Exception)

class AdaptiveCircuitBreaker:
    def __init__(self, name: str, config: CircuitBreakerConfig = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0
        self.fallback_cache = {}
        
    async def call(self, 
                   func: Callable, 
                   fallback_func: Callable = None,
                   cache_key: str = None,
                   *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection"""
        
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time < self.config.recovery_timeout:
                # Circuit is open, use fallback
                return await self._execute_fallback(fallback_func, cache_key, *args, **kwargs)
            else:
                # Try recovery
                self.state = CircuitState.HALF_OPEN
                self.success_count = 0
        
        try:
            # Execute with timeout
            result = await asyncio.wait_for(
                func(*args, **kwargs), 
                timeout=self.config.timeout_seconds
            )
            
            await self._on_success(result, cache_key)
            return result
            
        except self.config.expected_exception_types as e:
            await self._on_failure(e, fallback_func, cache_key, *args, **kwargs)
            raise
    
    async def _on_success(self, result: Any, cache_key: str = None):
        """Handle successful execution"""
        self.failure_count = 0
        
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitState.CLOSED
                logger.info(f"Circuit breaker {self.name} closed - service recovered")
        
        # Cache successful result for fallback
        if cache_key:
            self.fallback_cache[cache_key] = {
                'result': result,
                'timestamp': time.time(),
                'ttl': 300  # 5 minutes
            }
    
    async def _on_failure(self, 
                         exception: Exception, 
                         fallback_func: Callable,
                         cache_key: str,
                         *args, **kwargs):
        """Handle failed execution"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.config.failure_threshold:
            if self.state == CircuitState.CLOSED:
                self.state = CircuitState.OPEN
                logger.error(f"Circuit breaker {self.name} opened - service failing")
            elif self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                logger.warning(f"Circuit breaker {self.name} back to open - recovery failed")
        
        # Try fallback
        if fallback_func or cache_key:
            try:
                return await self._execute_fallback(fallback_func, cache_key, *args, **kwargs)
            except Exception as fallback_error:
                logger.error(f"Fallback also failed for {self.name}: {fallback_error}")
                raise exception  # Re-raise original exception
        
        raise exception
    
    async def _execute_fallback(self, 
                               fallback_func: Callable,
                               cache_key: str,
                               *args, **kwargs) -> Any:
        """Execute fallback strategy"""
        
        # Try cached result first
        if cache_key and cache_key in self.fallback_cache:
            cached = self.fallback_cache[cache_key]
            if time.time() - cached['timestamp'] < cached['ttl']:
                logger.info(f"Using cached fallback for {self.name}")
                return cached['result']
        
        # Try fallback function
        if fallback_func:
            logger.info(f"Using fallback function for {self.name}")
            return await fallback_func(*args, **kwargs)
        
        # No fallback available
        raise Exception(f"Service {self.name} unavailable and no fallback configured")

# Usage in feature store
class ResilientFeatureService:
    def __init__(self):
        self.db_circuit_breaker = AdaptiveCircuitBreaker("database")
        self.cache_circuit_breaker = AdaptiveCircuitBreaker("redis_cache")
        self.bigquery_circuit_breaker = AdaptiveCircuitBreaker("bigquery")
        
    async def get_user_features_resilient(self, user_id: str) -> Dict[str, Any]:
        """Get user features with multiple fallback layers"""
        
        # Try cache first (fastest)
        try:
            cached_features = await self.cache_circuit_breaker.call(
                self._get_from_cache,
                fallback_func=None,
                cache_key=f"cache_fallback:{user_id}",
                user_id
            )
            if cached_features:
                return cached_features
        except Exception:
            logger.warning(f"Cache unavailable for user {user_id}")
        
        # Try database (more reliable)
        try:
            db_features = await self.db_circuit_breaker.call(
                self._get_from_database,
                fallback_func=self._get_minimal_features,
                cache_key=f"db_fallback:{user_id}",
                user_id
            )
            return db_features
        except Exception:
            logger.error(f"Database unavailable for user {user_id}")
            
        # Final fallback - minimal safe features
        return await self._get_minimal_features(user_id)
    
    async def _get_minimal_features(self, user_id: str) -> Dict[str, Any]:
        """Minimal safe features for when all services are down"""
        return {
            'user_id': user_id,
            'user_features': {
                'age': 25,  # Safe default
                'location_country': 'unknown',
                'verification_status': 'unverified'
            },
            'transaction_features': {
                'total_orders': 0,
                'avg_order_value': 0,
                'last_order_days': 999
            },
            'risk_features': {
                'risk_score': 0.8,  # Conservative default - decline risky transactions
                'fraud_probability': 0.5,
                'payment_delays': 0,
                'device_trust_score': 0.3
            },
            'fallback': True,
            'fallback_reason': 'all_services_unavailable'
        }Feature Store Evolution PatternsFeature Versioning & A/B TestingFeature Version Management:from dataclasses import dataclass
from typing import Dict, Any, Optional, List
import hashlib
import random

@dataclass
class FeatureVersion:
    version_id: str
    name: str
    description: str
    computation_logic: str
    rollout_percentage: float
    is_active: bool
    created_at: float

class FeatureVersionManager:
    def __init__(self):
        self.feature_versions = {}
        self.user_assignments = {}  # Consistent user->version mapping
        self.performance_metrics = {}
        
    def register_feature_version(self, 
                                feature_name: str,
                                version: FeatureVersion):
        """Register a new feature version"""
        if feature_name not in self.feature_versions:
            self.feature_versions[feature_name] = {}
        
        self.feature_versions[feature_name][version.version_id] = version
        
        # Initialize performance tracking
        self.performance_metrics[f"{feature_name}:{version.version_id}"] = {
            'request_count': 0,
            'error_count': 0,
            'latency_sum': 0,
            'approval_rate': 0
        }
    
    def get_feature_version_for_user(self, 
                                   feature_name: str, 
                                   user_id: str) -> Optional[FeatureVersion]:
        """Get consistent feature version for user (A/B testing)"""
        
        if feature_name not in self.feature_versions:
            return None
        
        # Consistent assignment based on user_id hash
        assignment_key = f"{feature_name}:{user_id}"
        
        if assignment_key in self.user_assignments:
            version_id = self.user_assignments[assignment_key]
            return self.feature_versions[feature_name].get(version_id)
        
        # New assignment - use weighted selection
        active_versions = [v for v in self.feature_versions[feature_name].values() 
                          if v.is_active]
        
        if not active_versions:
            return None
        
        # Deterministic selection based on user hash
        user_hash = int(hashlib.md5(user_id.encode()).hexdigest()[:8], 16)
        selection_value = (user_hash % 10000) / 10000  # 0.0 to 1.0
        
        cumulative_percentage = 0
        for version in active_versions:
            cumulative_percentage += version.rollout_percentage
            if selection_value <= cumulative_percentage:
                self.user_assignments[assignment_key] = version.version_id
                return version
        
        # Fallback to first version
        selected_version = active_versions[0]
        self.user_assignments[assignment_key] = selected_version.version_id
        return selected_version
    
    async def compute_feature_with_version(self, 
                                         feature_name: str,
                                         user_id: str,
                                         base_data: Dict[str, Any]) -> Dict[str, Any]:
        """Compute feature using appropriate version"""
        
        version = self.get_feature_version_for_user(feature_name, user_id)
        if not version:
            raise ValueError(f"No active version found for feature {feature_name}")
        
        start_time = time.time()
        metrics_key = f"{feature_name}:{version.version_id}"
        
        try:
            # Execute version-specific computation logic
            if version.version_id == "risk_score_v1":
                result = await self._compute_risk_score_v1(base_data)
            elif version.version_id == "risk_score_v2":
                result = await self._compute_risk_score_v2(base_data)
            elif version.version_id == "risk_score_ml_enhanced":
                result = await self._compute_risk_score_ml(base_data)
            else:
                raise ValueError(f"Unknown version: {version.version_id}")
            
            # Track performance metrics
            latency = (time.time() - start_time) * 1000
            self.performance_metrics[metrics_key]['request_count'] += 1
            self.performance_metrics[metrics_key]['latency_sum'] += latency
            
            # Add version metadata
            result['_feature_version'] = {
                'version_id': version.version_id,
                'name': version.name,
                'computation_time_ms': round(latency, 2)
            }
            
            return result
            
        except Exception as e:
            self.performance_metrics[metrics_key]['error_count'] += 1
            raise
    
    async def _compute_risk_score_v1(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Original risk score computation"""
        payment_delays = data.get('payment_delays', 0)
        total_orders = data.get('total_orders', 0)
        
        base_risk = 0.5
        if payment_delays > 0:
            base_risk += min(payment_delays * 0.1, 0.3)
        if total_orders > 10:
            base_risk -= min((total_orders - 10) * 0.01, 0.2)
            
        return {'risk_score': min(max(base_risk, 0.0), 1.0)}
    
    async def _compute_risk_score_v2(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced risk score with more factors"""
        payment_delays = data.get('payment_delays', 0)
        total_orders = data.get('total_orders', 0)
        avg_order_value = data.get('avg_order_value', 0)
        days_since_signup = data.get('days_since_signup', 0)
        
        base_risk = 0.4  # Lower starting point
        
        # Payment history factor
        if payment_delays > 0:
            base_risk += min(payment_delays * 0.08, 0.25)
        
        # Experience factor
        if total_orders > 5:
            base_risk -= min((total_orders - 5) * 0.012, 0.15)
        
        # Order value factor (higher values = lower risk for established users)
        if total_orders > 3 and avg_order_value > 100:
            base_risk -= min((avg_order_value - 100) * 0.0001, 0.1)
        
        # Tenure factor
        if days_since_signup > 90:
            base_risk -= min((days_since_signup - 90) * 0.0005, 0.1)
            
        return {'risk_score': min(max(base_risk, 0.0), 1.0)}
    
    async def _compute_risk_score_ml(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """ML-enhanced risk score (placeholder for actual ML model)"""
        # In real implementation, this would call ML model inference
        # For now, simulate with enhanced logic
        
        features = [
            data.get('payment_delays', 0) / 10,
            min(data.get('total_orders', 0) / 50, 1.0),
            min(data.get('avg_order_value', 0) / 1000, 1.0),
            min(data.get('days_since_signup', 0) / 365, 1.0)
        ]
        
        # Simulate ML model output
        ml_risk_score = 0.5 + sum(features) * 0.1 - (features[1] + features[3]) * 0.2
        
        return {
            'risk_score': min(max(ml_risk_score, 0.0), 1.0),
            'feature_importance': {
                'payment_delays': features[0] * 0.4,
                'order_history': features[1] * 0.3,
                'order_value': features[2] * 0.2,
                'tenure': features[3] * 0.1
            }
        }
    
    def get_version_performance_comparison(self, feature_name: str) -> Dict[str, Any]:
        """Compare performance across feature versions"""
        if feature_name not in self.feature_versions:
            return {}
        
        comparison = {}
        for version_id, version in self.feature_versions[feature_name].items():
            metrics_key = f"{feature_name}:{version_id}"
            metrics = self.performance_metrics.get(metrics_key, {})
            
            request_count = metrics.get('request_count', 0)
            if request_count > 0:
                comparison[version_id] = {
                    'version_name': version.name,
                    'rollout_percentage': version.rollout_percentage,
                    'request_count': request_count,
                    'error_rate': metrics.get('error_count', 0) / request_count,
                    'avg_latency_ms': metrics.get('latency_sum', 0) / request_count,
                    'approval_rate': metrics.get('approval_rate', 0)
                }
        
        return comparisonReal-time Feature EngineeringStream Processing for Real-time Features:import asyncio
from typing import AsyncIterator
import json
from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass
class StreamingEvent:
    user_id: str
    event_type: str
    data: Dict[str, Any]
    timestamp: float

class RealTimeFeatureProcessor:
    def __init__(self):
        self.feature_windows = {
            '5m': timedelta(minutes=5),
            '1h': timedelta(hours=1),
            '24h': timedelta(hours=24)
        }
        self.sliding_windows = {}
        self.feature_cache = {}
        
    async def process_event_stream(self, event_stream: AsyncIterator[StreamingEvent]):
        """Process streaming events for real-time feature updates"""
        
        async for event in event_stream:
            try:
                await self._update_sliding_windows(event)
                await self._compute_real_time_features(event)
                await self._trigger_feature_updates(event)
                
            except Exception as e:
                logger.error(f"Error processing streaming event: {e}", extra={
                    'user_id': event.user_id,
                    'event_type': event.event_type,
                    'timestamp': event.timestamp
                })
    
    async def _update_sliding_windows(self, event: StreamingEvent):
        """Update sliding window aggregations"""
        user_id = event.user_id
        
        if user_id not in self.sliding_windows:
            self.sliding_windows[user_id] = {
                'transactions': [],
                'payments': [],
                'risk_events': []
            }
        
        # Add event to appropriate window
        if event.event_type == 'transaction_completed':
            self.sliding_windows[user_id]['transactions'].append({
                'amount': event.data.get('amount', 0),
                'merchant': event.data.get('merchant'),
                'timestamp': event.timestamp
            })
        elif event.event_type == 'payment_made':
            self.sliding_windows[user_id]['payments'].append({
                'amount': event.data.get('amount', 0),
                'status': event.data.get('status'),
                'timestamp': event.timestamp
            })
        
        # Clean old events outside window
        await self._clean_expired_events(user_id)
    
    async def _compute_real_time_features(self, event: StreamingEvent):
        """Compute real-time features from sliding windows"""
        user_id = event.user_id
        current_time = event.timestamp
        
        if user_id not in self.sliding_windows:
            return
        
        windows = self.sliding_windows[user_id]
        real_time_features = {}
        
        # Velocity features (last 5 minutes)
        recent_transactions = [
            t for t in windows['transactions']
            if current_time - t['timestamp'] <= self.feature_windows['5m'].total_seconds()
        ]
        
        real_time_features['transaction_velocity_5m'] = len(recent_transactions)
        real_time_features['spending_velocity_5m'] = sum(t['amount'] for t in recent_transactions)
        
        # Hourly patterns
        hourly_transactions = [
            t for t in windows['transactions']
            if current_time - t['timestamp'] <= self.feature_windows['1h'].total_seconds()
        ]
        
        real_time_features['transactions_last_hour'] = len(hourly_transactions)
        real_time_features['unique_merchants_last_hour'] = len(set(
            t['merchant'] for t in hourly_transactions if t['merchant']
        ))
        
        # Payment behavior
        recent_payments = [
            p for p in windows['payments']
            if current_time - p['timestamp'] <= self.feature_windows['24h'].total_seconds()
        ]
        
        on_time_payments = sum(1 for p in recent_payments if p['status'] == 'completed')
        total_payments = len(recent_payments)
        
        real_time_features['payment_success_rate_24h'] = (
            on_time_payments / total_payments if total_payments > 0 else 1.0
        )
        
        # Update feature cache
        cache_key = f"realtime_features:{user_id}"
        self.feature_cache[cache_key] = {
            'features': real_time_features,
            'last_updated': current_time,
            'ttl': 300  # 5 minutes
        }
        
        # Push to Redis for immediate availability
        await self._push_to_cache(cache_key, real_time_features)
    
    async def get_real_time_features(self, user_id: str) -> Dict[str, Any]:
        """Get current real-time features for user"""
        cache_key = f"realtime_features:{user_id}"
        
        # Check local cache first
        if cache_key in self.feature_cache:
            cached = self.feature_cache[cache_key]
            if time.time() - cached['last_updated'] < cached['ttl']:
                return cached['features']
        
        # Fall back to Redis
        redis_data = await redis_client.get(cache_key)
        if redis_data:
            return json.loads(redis_data)
        
        # No real-time data available
        return {
            'transaction_velocity_5m': 0,
            'spending_velocity_5m': 0,
            'transactions_last_hour': 0,
            'unique_merchants_last_hour': 0,
            'payment_success_rate_24h': 1.0
        