"""
Real-time Feature Pipeline
Streaming feature updates via Kafka/Pub-Sub
"""

import asyncio
import json
import time
from datetime import datetime
from typing import Dict, Any, Optional, List

import structlog
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import Counter, Histogram

from src.config.settings import get_settings
from src.feature_store.cache import cache
from src.feature_store import database

logger = structlog.get_logger(__name__)

# Metrics
STREAMING_MESSAGES = Counter(
    'feature_store_streaming_messages_total',
    'Streaming messages processed',
    ['topic', 'status']
)

STREAMING_LATENCY = Histogram(
    'feature_store_streaming_latency_seconds',
    'Streaming message processing latency',
    ['topic']
)


class FeaturePipeline:
    """Real-time feature update pipeline"""

    def __init__(self):
        self.settings = get_settings()
        self.consumer: Optional[KafkaConsumer] = None
        self.producer: Optional[KafkaProducer] = None
        self.running = False

    async def start(self):
        """Start streaming pipeline"""
        logger.info("Starting feature streaming pipeline")

        try:
            # Initialize Kafka consumer
            self.consumer = KafkaConsumer(
                self.settings.kafka_topic_features,
                bootstrap_servers=self.settings.kafka_bootstrap_servers.split(','),
                group_id=f"{self.settings.kafka_group_id or 'feature-store'}-consumer",
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                max_poll_records=100
            )

            # Initialize Kafka producer
            self.producer = KafkaProducer(
                bootstrap_servers=self.settings.kafka_bootstrap_servers.split(','),
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks='all',
                retries=3
            )

            self.running = True

            # Start processing loop
            await self._process_messages()

        except Exception as e:
            logger.error(f"Failed to start streaming pipeline: {e}")
            raise

    async def stop(self):
        """Stop streaming pipeline"""
        logger.info("Stopping feature streaming pipeline")

        self.running = False

        if self.consumer:
            self.consumer.close()

        if self.producer:
            self.producer.close()

    async def _process_messages(self):
        """Process streaming messages"""
        while self.running:
            try:
                if not self.consumer:
                    await asyncio.sleep(1)
                    continue

                # Poll for messages
                message_batch = self.consumer.poll(timeout_ms=1000)

                if not message_batch:
                    await asyncio.sleep(0.1)
                    continue

                # Process each partition's messages
                for topic_partition, messages in message_batch.items():
                    await self._process_partition_messages(messages)

                # Commit offsets
                self.consumer.commit_async()

            except Exception as e:
                logger.error(f"Error processing messages: {e}")
                await asyncio.sleep(5)  # Back off on error

    async def _process_partition_messages(self, messages):
        """Process messages from a single partition"""
        for message in messages:
            start_time = time.time()

            try:
                # Parse message
                event_data = message.value
                event_type = event_data.get('event_type')
                user_id = event_data.get('user_id')

                if not event_type or not user_id:
                    logger.warning("Invalid message format", message=event_data)
                    STREAMING_MESSAGES.labels(topic=message.topic, status='invalid').inc()
                    continue

                # Process based on event type
                success = await self._process_feature_event(event_type, event_data)

                # Record metrics
                latency = time.time() - start_time
                STREAMING_LATENCY.labels(topic=message.topic).observe(latency)
                STREAMING_MESSAGES.labels(
                    topic=message.topic,
                    status='success' if success else 'error'
                ).inc()

                logger.debug(
                    "Processed streaming message",
                    event_type=event_type,
                    user_id=user_id,
                    latency_ms=latency * 1000
                )

            except Exception as e:
                logger.error(f"Error processing message: {e}", message=message.value)
                STREAMING_MESSAGES.labels(topic=message.topic, status='error').inc()

    async def _process_feature_event(self, event_type: str, event_data: Dict[str, Any]) -> bool:
        """Process individual feature event"""
        try:
            user_id = event_data['user_id']

            if event_type == 'user_updated':
                return await self._handle_user_update(user_id, event_data)

            elif event_type == 'transaction_completed':
                return await self._handle_transaction_update(user_id, event_data)

            elif event_type == 'payment_status_changed':
                return await self._handle_payment_update(user_id, event_data)

            elif event_type == 'risk_score_updated':
                return await self._handle_risk_update(user_id, event_data)

            else:
                logger.warning(f"Unknown event type: {event_type}")
                return False

        except Exception as e:
            logger.error(f"Error processing {event_type} event: {e}")
            return False

    async def _handle_user_update(self, user_id: str, event_data: Dict[str, Any]) -> bool:
        """Handle user profile update event"""
        try:
            # Invalidate cache
            await cache.delete_user_features(user_id)

            # Optionally trigger immediate recomputation
            if event_data.get('recompute_features', False):
                await self._trigger_feature_recomputation(user_id, ['user'])

            return True

        except Exception as e:
            logger.error(f"Error handling user update: {e}")
            return False

    async def _handle_transaction_update(self, user_id: str, event_data: Dict[str, Any]) -> bool:
        """Handle transaction completion event"""
        try:
            # Invalidate transaction and risk features cache
            await cache.delete_user_features(user_id)

            # Update real-time transaction counters
            await self._update_realtime_counters(user_id, event_data)

            return True

        except Exception as e:
            logger.error(f"Error handling transaction update: {e}")
            return False

    async def _handle_payment_update(self, user_id: str, event_data: Dict[str, Any]) -> bool:
        """Handle payment status change event"""
        try:
            payment_status = event_data.get('status')

            if payment_status in ['failed', 'delayed']:
                # Invalidate risk features immediately
                await cache.delete_user_features(user_id)

                # Trigger risk score recalculation
                await self._trigger_feature_recomputation(user_id, ['risk'])

            return True

        except Exception as e:
            logger.error(f"Error handling payment update: {e}")
            return False

    async def _handle_risk_update(self, user_id: str, event_data: Dict[str, Any]) -> bool:
        """Handle risk score update event"""
        try:
            # Invalidate risk features cache
            await cache.delete_user_features(user_id)

            # Update risk score in database
            new_risk_score = event_data.get('risk_score')
            if new_risk_score is not None:
                await self._update_risk_score(user_id, new_risk_score)

            return True

        except Exception as e:
            logger.error(f"Error handling risk update: {e}")
            return False

    async def _update_realtime_counters(self, user_id: str, event_data: Dict[str, Any]):
        """Update real-time transaction counters"""
        # This would update counters in Redis or a similar fast store
        # For simplicity, we'll invalidate cache and let it refresh from DB
        pass

    async def _update_risk_score(self, user_id: str, risk_score: float):
        """Update risk score in database"""
        try:
            query = """
                UPDATE risk_features 
                SET risk_score = $2, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = $1
            """

            db = getattr(database, "db", None)
            if db and getattr(db, "pool", None):
                async with db.pool.acquire() as conn:
                    await conn.execute(query, user_id, risk_score)

        except Exception as e:
            logger.error(f"Error updating risk score: {e}")

    async def _trigger_feature_recomputation(self, user_id: str, feature_types: List[str]):
        """Trigger immediate feature recomputation"""
        try:
            # Send message to trigger recomputation
            recompute_event = {
                'event_type': 'recompute_features',
                'user_id': user_id,
                'feature_types': feature_types,
                'timestamp': datetime.utcnow().isoformat(),
                'priority': 'high'
            }

            if self.producer:
                self.producer.send(
                    f"{self.settings.kafka_topic_features}-recompute",
                    value=recompute_event
                )

        except Exception as e:
            logger.error(f"Error triggering recomputation: {e}")

    async def publish_feature_update(self, user_id: str, feature_type: str, data: Dict[str, Any]):
        """Publish feature update event"""
        try:
            event = {
                'event_type': 'feature_updated',
                'user_id': user_id,
                'feature_type': feature_type,
                'data': data,
                'timestamp': datetime.utcnow().isoformat()
            }

            if self.producer:
                self.producer.send(
                    self.settings.kafka_topic_features,
                    value=event
                )

                logger.debug(f"Published feature update: {user_id} - {feature_type}")

        except Exception as e:
            logger.error(f"Error publishing feature update: {e}")


# Global pipeline instance
feature_pipeline = FeaturePipeline()
