# test_grpc.py
import unittest
from unittest.mock import MagicMock
import grpc
from src.proto import feature_store_pb2, feature_store_pb2_grpc


class TestFeatureStoreGRPC(unittest.TestCase):
    def setUp(self):
        # Create a fake gRPC channel
        self.channel = MagicMock(spec=grpc.Channel)
        self.stub = feature_store_pb2_grpc.FeatureStoreStub(self.channel)

    def test_get_user_features(self):
        request = feature_store_pb2.UserFeatureRequest(
            user_id="user123",
            feature_types=["demographics", "behavior"],
            include_metadata=True
        )

        response = feature_store_pb2.UserFeatureResponse(
            uid="user123",
            demographics=feature_store_pb2.UserFeatures(age=30, location_country="US", location_city="NY", total_orders=10, avg_order_value=100.0, days_since_first_order=365, preferred_payment_method="card", account_verified=True),
            behavior=feature_store_pb2.UserFeatures(),
            risk=feature_store_pb2.UserFeatures(),
            response_time=100,
            cache_hit=True,
            freshness_ms=50,
            timestamp=1234567890
        )

        # Mock the channel call
        self.channel.unary_unary.return_value = MagicMock(return_value=response)

        result = self.stub.GetUserFeatures(request)
        self.assertEqual(result.uid, "user123")
        self.assertTrue(result.cache_hit)

    def test_get_batch_features(self):
        user_request = feature_store_pb2.UserFeatureRequest(user_id="user123")
        batch_request = feature_store_pb2.BatchFeatureRequest(requests=[user_request])
        user_response = feature_store_pb2.UserFeatureResponse(uid="user123")
        batch_response = feature_store_pb2.BatchFeatureResponse(
            total_requests=1,
            successful_requests=1,
            failed_requests=0,
            total_response_time_ms=100,
            cache_hit_ratio=1.0,
            responses=[user_response]
        )

        # Mock the channel call
        self.channel.unary_unary.return_value = MagicMock(return_value=batch_response)

        result = self.stub.GetBatchFeatures(batch_request)
        self.assertEqual(result.total_requests, 1)
        self.assertEqual(len(result.responses), 1)

    def test_health_check(self):
        health_request = feature_store_pb2.HealthCheckRequest(service="feature_store")
        health_response = feature_store_pb2.HealthCheckResponse(
            status="OK",
            timestamp=1234567890,
            version="1.0.0"
        )

        # Mock the channel call
        self.channel.unary_unary.return_value = MagicMock(return_value=health_response)

        result = self.stub.HealthCheck(health_request)
        self.assertEqual(result.status, "OK")
        self.assertEqual(result.version, "1.0.0")


if __name__ == "__main__":
    unittest.main()
