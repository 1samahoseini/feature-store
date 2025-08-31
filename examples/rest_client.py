"""
Example REST API client for Feature Store
Demonstrates how to integrate with the feature store using REST API
"""

import asyncio
from typing import List, Dict, Any, Optional

import httpx
import structlog

logger = structlog.get_logger(__name__)


class FeatureStoreClient:
    """REST client for Feature Store API"""

    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "FeatureStoreClient":
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout,
            headers={
                'Content-Type': 'application/json',
                'User-Agent': 'FeatureStore-Python-Client/1.0.0'
            }
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> Optional[bool]:
        if self.client:
            await self.client.aclose()
        return None

    async def get_user_features(
        self,
        user_id: str,
        feature_types: Optional[List[str]] = None,
        include_metadata: bool = False
    ) -> Dict[str, Any]:
        """Get features for a single user"""
        assert self.client is not None, "Client not initialized"

        params: Dict[str, Any] = {}
        if feature_types:
            params['feature_types'] = feature_types
        if include_metadata:
            params['include_metadata'] = include_metadata

        try:
            response = await self.client.get(f"/api/v1/features/user/{user_id}", params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error getting user features: {e.response.status_code}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error getting user features: {e}")
            raise

    async def get_batch_features(
        self,
        requests: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Get features for multiple users in batch"""
        assert self.client is not None, "Client not initialized"

        if len(requests) > 100:
            raise ValueError("Batch size cannot exceed 100 requests")

        batch_request = {"requests": requests}

        try:
            response = await self.client.post("/api/v1/features/batch", json=batch_request)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error getting batch features: {e.response.status_code}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error getting batch features: {e}")
            raise

    async def invalidate_user_cache(self, user_id: str) -> bool:
        """Invalidate cached features for a user"""
        assert self.client is not None, "Client not initialized"

        try:
            response = await self.client.delete(f"/api/v1/features/user/{user_id}")
            response.raise_for_status()
            return True
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error invalidating cache: {e.response.status_code}")
            return False
        except httpx.RequestError as e:
            logger.error(f"Request error invalidating cache: {e}")
            return False

    async def refresh_features(self, user_ids: List[str]) -> Dict[str, Any]:
        """Refresh features for multiple users"""
        assert self.client is not None, "Client not initialized"

        if len(user_ids) > 1000:
            raise ValueError("Cannot refresh more than 1000 users at once")

        try:
            response = await self.client.post("/api/v1/features/refresh", json={"user_ids": user_ids})
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error refreshing features: {e.response.status_code}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error refreshing features: {e}")
            raise

    async def get_health_status(self) -> Dict[str, Any]:
        """Get feature store health status"""
        assert self.client is not None, "Client not initialized"

        try:
            response = await self.client.get("/api/v1/features/health")
            return {
                'status_code': response.status_code,
                'data': response.json()
            }
        except httpx.RequestError as e:
            logger.error(f"Request error getting health status: {e}")
            raise

    async def get_statistics(self) -> Dict[str, Any]:
        """Get feature store statistics"""
        assert self.client is not None, "Client not initialized"

        try:
            response = await self.client.get("/api/v1/features/stats")
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error getting statistics: {e.response.status_code}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error getting statistics: {e}")
            raise


# ======================
# Example usage functions
# ======================

async def example_single_user_request():
    async with FeatureStoreClient("http://localhost:8000") as client:
        features = await client.get_user_features(
            user_id="example_user_001",
            feature_types=["user", "transaction", "risk"],
            include_metadata=True
        )
        print(json.dumps(features, indent=2))


async def example_batch_request():
    async with FeatureStoreClient("http://localhost:8000") as client:
        batch_requests = [
            {"user_id": f"batch_user_{i:03d}", "feature_types": ["user", "risk"]}
            for i in range(1, 11)
        ]
        batch_response = await client.get_batch_features(batch_requests)
        print(json.dumps(batch_response, indent=2))


# ======================
# Run examples
# ======================

if __name__ == "__main__":
    import json
    asyncio.run(example_single_user_request())
    asyncio.run(example_batch_request())
