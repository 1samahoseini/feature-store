#!/usr/bin/env python3
"""
Internal Service Integration Example
Demonstrates how internal microservices integrate with the Feature Store via gRPC

Use case: Risk scoring service making real-time credit decisions
Performance target: <30ms P95 latency via gRPC
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Tuple, TypedDict, Union
from grpc import aio as aio_grpc
from dataclasses import dataclass

# Feature Store gRPC imports
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src/proto'))


from feature_store_pb2 import UserFeatureRequest, HealthCheckRequest  # type: ignore
from feature_store_pb2_grpc import FeatureStoreStub  # type: ignore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class CreditDecision:
    """Credit decision response"""
    user_id: str
    decision: str  # "approved", "declined", "manual_review"
    credit_limit: float
    confidence_score: float
    decision_factors: Dict[str, float]
    processing_time_ms: float
    feature_freshness_ms: int


class RiskEvaluationResult(TypedDict):
    decision: str
    credit_limit: float
    confidence: float
    factors: Dict[str, float]
    composite_risk: float


class RiskScoringService:
    """Internal risk scoring service that consumes features via gRPC"""

    def __init__(self, feature_store_endpoint: str = "localhost:50051") -> None:
        self.feature_store_endpoint: str = feature_store_endpoint
        self.channel: Optional[aio_grpc.Channel] = None
        self.client: Optional[FeatureStoreStub] = None

        self.risk_thresholds: Dict[str, float] = {
            "auto_approve": 0.15,
            "auto_decline": 0.45,
            "max_payment_delays": 2,
            "min_credit_age_months": 3
        }

        self.credit_limits: Dict[str, Dict[str, int]] = {
            "premium": {"min": 5000, "max": 25000},
            "regular": {"min": 1000, "max": 10000},
            "new": {"min": 200, "max": 2000}
        }

    async def __aenter__(self) -> "RiskScoringService":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.disconnect()

    async def connect(self) -> None:
        """Establish gRPC connection to Feature Store"""
        try:
            self.channel = aio_grpc.insecure_channel(self.feature_store_endpoint)
            self.client = FeatureStoreStub(self.channel)

            health_request = HealthCheckRequest()
            assert self.client is not None
            health_response = await self.client.HealthCheck(health_request)

            logger.info(f"Connected to Feature Store: {health_response.status}")
            logger.info(f"Feature Store version: {health_response.version}")

        except Exception as e:
            logger.error(f"Failed to connect to Feature Store: {e}")
            raise

    async def disconnect(self) -> None:
        """Close gRPC connection"""
        if self.channel:
            await self.channel.close()
            logger.info("Disconnected from Feature Store")

    async def make_credit_decision(self, user_id: str, requested_amount: float) -> CreditDecision:
        """Make real-time credit decision using Feature Store"""
        start_time = time.time()
        try:
            feature_request = UserFeatureRequest(uid=user_id)
            assert self.client is not None
            feature_response = await self.client.GetUserFeatures(feature_request)

            demographics = feature_response.demographics
            behavior = feature_response.behavior
            risk = feature_response.risk

            logger.info(
                f"Features retrieved for {user_id}: cache_hit={feature_response.cache_hit}, "
                f"response_time={feature_response.response_time:.1f}ms"
            )

            decision_result = self._evaluate_credit_risk(demographics, behavior, risk, requested_amount)
            processing_time = (time.time() - start_time) * 1000

            return CreditDecision(
                user_id=user_id,
                decision=decision_result["decision"],
                credit_limit=decision_result["credit_limit"],
                confidence_score=decision_result["confidence"],
                decision_factors=decision_result["factors"],
                processing_time_ms=round(processing_time, 1),
                feature_freshness_ms=feature_response.freshness_ms
            )

        except Exception as e:
            logger.error(f"Credit decision failed for {user_id}: {e}")
            processing_time = (time.time() - start_time) * 1000
            return CreditDecision(
                user_id=user_id,
                decision="manual_review",
                credit_limit=0.0,
                confidence_score=0.0,
                decision_factors={"error": 1.0},
                processing_time_ms=round(processing_time, 1),
                feature_freshness_ms=0
            )

    def _evaluate_credit_risk(self, demographics, behavior, risk, requested_amount: float) -> RiskEvaluationResult:
        """Internal risk evaluation logic"""
        factors: Dict[str, float] = {}

        default_prob = risk.default_probability
        factors["default_probability"] = default_prob

        payment_delay_score = min(risk.payment_delays / 5.0, 1.0)
        factors["payment_history"] = payment_delay_score

        utilization_score = min(risk.utilization_rate, 1.0)
        factors["utilization"] = utilization_score

        credit_age_score = max(0, (risk.credit_age - 1) / 24.0)
        factors["account_maturity"] = 1.0 - credit_age_score

        order_consistency = min(behavior.total_orders / 10.0, 1.0)
        factors["order_consistency"] = 1.0 - order_consistency

        age_risk = 0.3 if demographics.age < 22 else 0.1 if demographics.age > 45 else 0.2
        factors["age_risk"] = age_risk

        composite_risk = (
            default_prob * 0.4 +
            payment_delay_score * 0.2 +
            utilization_score * 0.15 +
            factors["account_maturity"] * 0.1 +
            factors["order_consistency"] * 0.1 +
            age_risk * 0.05
        )

        segment = demographics.segment
        base_limits = self.credit_limits.get(segment, self.credit_limits["new"])

        if composite_risk < self.risk_thresholds["auto_approve"]:
            credit_limit = base_limits["max"] * (1.0 - composite_risk)
            decision = "approved" if requested_amount <= credit_limit else "manual_review"
            confidence = 0.9 - composite_risk
        elif composite_risk > self.risk_thresholds["auto_decline"]:
            credit_limit = 0.0
            decision = "declined"
            confidence = composite_risk
        else:
            credit_limit = base_limits["min"] * (1.2 - composite_risk)
            if requested_amount <= credit_limit and risk.payment_delays <= self.risk_thresholds["max_payment_delays"]:
                decision = "approved"
                confidence = 0.7 - composite_risk
            else:
                decision = "manual_review"
                confidence = 0.5

        return RiskEvaluationResult(
            decision=decision,
            credit_limit=round(credit_limit, 2),
            confidence=round(confidence, 3),
            factors=factors,
            composite_risk=round(composite_risk, 3)
        )


class BatchDecisionProcessor:
    """Process multiple credit decisions efficiently"""

    def __init__(self, risk_service: RiskScoringService) -> None:
        self.risk_service = risk_service

    async def process_batch(self, requests: List[Tuple[str, float]]) -> List[CreditDecision]:
        """Process multiple credit decisions concurrently"""
        logger.info(f"Processing batch of {len(requests)} credit decisions")
        tasks = [self.risk_service.make_credit_decision(user_id, amount) for user_id, amount in requests]

        start_time = time.time()
        results: List[Union[CreditDecision, BaseException]] = await asyncio.gather(*tasks, return_exceptions=True)
        total_time = (time.time() - start_time) * 1000

        decisions: List[CreditDecision] = []
        errors = 0

        for result in results:
            if isinstance(result, Exception):
                errors += 1
                logger.error(f"Batch processing error: {result}")
            elif isinstance(result, CreditDecision):
                decisions.append(result)

        avg_latency = total_time / len(requests) if requests else 0
        throughput = len(requests) / (total_time / 1000) if total_time > 0 else 0

        logger.info(f"Batch completed: {len(decisions)} decisions, {errors} errors")
        logger.info(f"Total time: {total_time:.1f}ms, Avg latency: {avg_latency:.1f}ms")
        logger.info(f"Throughput: {throughput:.1f} decisions/second")

        return decisions


async def simulate_real_traffic() -> None:
    """Simulate realistic traffic patterns for testing"""
    async with RiskScoringService() as risk_service:
        batch_processor = BatchDecisionProcessor(risk_service)
        scenarios: List[Tuple[str, List[Tuple[str, float]]]] = [
            ("Single high-value request", [("user_00000001", 5000.0)]),
            ("Small batch processing", [(f"user_{i:08d}", 1000.0) for i in range(1, 6)]),
            ("Peak traffic simulation", [(f"user_{i:08d}", 750.0) for i in range(1, 21)]),
        ]

        for scenario_name, requests in scenarios:
            logger.info(f"\n--- {scenario_name} ---")
            decisions = await batch_processor.process_batch(requests)

            approved = sum(1 for d in decisions if d.decision == "approved")
            declined = sum(1 for d in decisions if d.decision == "declined")
            manual_review = sum(1 for d in decisions if d.decision == "manual_review")
            avg_processing_time = sum(d.processing_time_ms for d in decisions) / len(decisions) if decisions else 0
            cache_hit_rate = sum(1 for d in decisions if d.feature_freshness_ms < 1000) / len(decisions) if decisions else 0

            logger.info(f"Results: {approved} approved, {declined} declined, {manual_review} manual review")
            logger.info(f"Avg processing time: {avg_processing_time:.1f}ms")
            logger.info(f"Feature cache hit rate: {cache_hit_rate:.1%}")

            if decisions:
                sample = decisions[0]
                logger.info(f"Sample decision for {sample.user_id}: {sample.decision} "
                            f"(limit: ${sample.credit_limit:.0f}, confidence: {sample.confidence_score:.1%})")
                logger.info(f"Key factors: {dict(list(sample.decision_factors.items())[:3])}")


async def main() -> int:
    """Main function demonstrating internal service integration"""
    logger.info("=" * 60)
    logger.info("Feature Store Internal Service Integration Demo")
    logger.info("=" * 60)

    try:
        logger.info("\n--- Single Credit Decision ---")
        async with RiskScoringService() as risk_service:
            decision = await risk_service.make_credit_decision("user_12345", 2500.0)
            logger.info(f"Decision: {decision.decision}")
            logger.info(f"Credit Limit: ${decision.credit_limit:.0f}")
            logger.info(f"Confidence: {decision.confidence_score:.1%}")
            logger.info(f"Processing Time: {decision.processing_time_ms:.1f}ms")
            logger.info(f"Feature Freshness: {decision.feature_freshness_ms}ms")

        logger.info("\n--- Traffic Simulation ---")
        await simulate_real_traffic()

        logger.info("\n" + "=" * 60)
        logger.info("Integration demo completed successfully!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Demo failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(asyncio.run(main()))
