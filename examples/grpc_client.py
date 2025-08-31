"""
gRPC Client Example for BNPL Feature Store
Demonstrates high-performance gRPC API usage for internal services
"""
import grpc  # type: ignore
import asyncio
import time
import json
from typing import List, Dict, Any, Optional

# Import generated protobuf code (type: ignore to silence missing stubs)
try:
    from src.proto import feature_store_pb2  # type: ignore
    from src.proto import feature_store_pb2_grpc  # type: ignore
except ImportError:
    print("Generated gRPC code not found. Run: make generate-grpc")
    exit(1)


class FeatureStoreGRPCClient:
    """High-performance gRPC client for feature store"""

    def __init__(self, server_address: str = "localhost:50051"):
        self.server_address: str = server_address
        self.channel: Optional[grpc.Channel] = None
        self.stub: Optional[Any] = None  # type: ignore

    def connect(self) -> None:
        """Establish gRPC connection"""
        self.channel = grpc.insecure_channel(self.server_address)
        self.stub = feature_store_pb2_grpc.FeatureStoreStub(self.channel)  # type: ignore
        print(f"Connected to gRPC server at {self.server_address}")

    def disconnect(self) -> None:
        """Close gRPC connection"""
        if self.channel:
            self.channel.close()
            print("Disconnected from gRPC server")

    def get_user_features(
        self, user_id: str, feature_types: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Get user features via gRPC (sub-30ms target)"""
        if not self.stub:
            raise RuntimeError("Not connected. Call connect() first.")

        feature_types = feature_types or ["demographics", "behavior", "risk"]

        request = feature_store_pb2.UserFeatureRequest(  # type: ignore
            user_id=user_id, feature_types=feature_types
        )

        start_time = time.time()
        try:
            response = self.stub.GetUserFeatures(request)  # type: ignore
            latency_ms = (time.time() - start_time) * 1000

            result = {
                "uid": getattr(response, "uid", ""),  # ✅ aligned with proto
                "features": self._parse_features(response),
                "latency_ms": round(latency_ms, 2),
                "cache_hit": getattr(response, "cache_hit", False),  # type: ignore
                "timestamp": getattr(response, "timestamp", 0),  # type: ignore
            }

            print(f"✅ gRPC Response in {latency_ms:.1f}ms (target: <30ms)")
            return result

        except grpc.RpcError as e:
            print(f"❌ gRPC Error: {e.code()} - {e.details()}")
            raise

    def get_batch_features(self, user_ids: List[str]) -> List[Dict[str, Any]]:
        """Get features for multiple users efficiently"""
        if not self.stub:
            raise RuntimeError("Not connected. Call connect() first.")

        requests = [
            feature_store_pb2.UserFeatureRequest(
                user_id=user_id,
                feature_types=["demographics", "behavior", "risk"]
            )
            for user_id in user_ids
        ]

        # ✅ fixed name (was BatchFeaturesRequest → BatchFeatureRequest)
        batch_request = feature_store_pb2.BatchFeatureRequest(requests=requests)  # type: ignore

        start_time = time.time()
        try:
            response = self.stub.GetBatchFeatures(batch_request)  # type: ignore
            total_latency_ms = (time.time() - start_time) * 1000
            avg_latency_ms = total_latency_ms / len(user_ids) if user_ids else 0

            results = []
            for user_response in getattr(response, "responses", []):  # type: ignore
                results.append({
                    "uid": getattr(user_response, "uid", ""),  # ✅ matches proto
                    "features": self._parse_features(user_response),
                    "cache_hit": getattr(user_response, "cache_hit", False),  # type: ignore
                })

            print(f"✅ Batch gRPC Response: {len(user_ids)} users in {total_latency_ms:.1f}ms")
            print(f"📊 Average latency: {avg_latency_ms:.1f}ms per user")
            return results

        except grpc.RpcError as e:
            print(f"❌ Batch gRPC Error: {e.code()} - {e.details()}")
            raise

    def _parse_features(self, response: Any) -> Dict[str, Any]:
        """Parse protobuf response to Python dict"""
        return {
            "demographics": {
                "age": getattr(response.demographics, "age", 0),
                "location_country": getattr(response.demographics, "location_country", ""),
                "location_city": getattr(response.demographics, "location_city", ""),
            },
            "behavior": {
                "total_orders": getattr(response.behavior, "total_orders", 0),
                "avg_order_value": getattr(response.behavior, "avg_order_value", 0.0),
                "days_since_first_order": getattr(response.behavior, "days_since_first_order", 0),
                "preferred_payment_method": getattr(response.behavior, "preferred_payment_method", ""),
            },
            "risk": {
                "account_verified": getattr(response.risk, "account_verified", False),
            }
        }


async def async_grpc_example():
    """Async gRPC client example for high concurrency"""
    import grpc.aio  # type: ignore

    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        stub = feature_store_pb2_grpc.FeatureStoreStub(channel)  # type: ignore
        user_ids = [f"user_{i:06d}" for i in range(1, 11)]
        tasks = [
            stub.GetUserFeatures(feature_store_pb2.UserFeatureRequest(
                user_id=user_id, feature_types=["demographics", "behavior", "risk"]
            ))  # type: ignore
            for user_id in user_ids
        ]

        start_time = time.time()
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        successful_responses = [r for r in responses if not isinstance(r, Exception)]
        end_time = time.time()

        print(f"🚀 Async gRPC: {len(successful_responses)} concurrent requests in {(end_time - start_time) * 1000:.1f}ms")


def main():
    """Demonstrate gRPC client usage"""
    print("🔌 BNPL Feature Store - gRPC Client Example")
    print("=" * 50)

    client = FeatureStoreGRPCClient()

    try:
        client.connect()

        print("\n1️⃣ Single User Feature Request:")
        user_features = client.get_user_features("user_000001")
        print(json.dumps(user_features, indent=2))

        print("\n2️⃣ Batch Feature Request:")
        batch_users = ["user_000001", "user_000002", "user_000003"]
        batch_results = client.get_batch_features(batch_users)
        for result in batch_results:
            print(f"User {result['uid']}: Cache Hit = {result['cache_hit']}")

        print("\n3️⃣ Performance Test (10 requests):")
        start_time = time.time()
        for i in range(10):
            client.get_user_features(f"user_{i+1:06d}")
        end_time = time.time()
        avg_latency = ((end_time - start_time) * 1000) / 10
        print(f"📈 Average latency: {avg_latency:.1f}ms per request")

        print("\n4️⃣ Async Concurrent Requests:")
        asyncio.run(async_grpc_example())

    except KeyboardInterrupt:
        print("\n⏹️ Interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
