"""
gRPC Server for Feature Store
High-performance binary protocol API
"""

import asyncio
from concurrent import futures

import grpc
import structlog
from prometheus_client import start_http_server

from src.config.settings import get_settings
from src.feature_store.grpc_service import FeatureStoreServicer
from src.proto import feature_store_pb2_grpc

# Configure logger
logger = structlog.get_logger(__name__)


async def serve():
    """Start gRPC server with Prometheus metrics."""
    settings = get_settings()

    # Create gRPC async server with thread pool
    server = grpc.aio.server(
        futures.ThreadPoolExecutor(max_workers=settings.max_workers)
    )

    # Register feature store service
    feature_store_pb2_grpc.add_FeatureStoreServicer_to_server(
        FeatureStoreServicer(),
        server
    )

    # Bind server to address
    listen_addr = f"[::]:{settings.grpc_port}"
    server.add_insecure_port(listen_addr)

    logger.info("Starting gRPC server", address=listen_addr, environment=settings.env)

    # Start Prometheus metrics for local development
    if settings.env == "local":
        metrics_port = 9091
        start_http_server(metrics_port)
        logger.info("Prometheus metrics server started", port=metrics_port)

    # Start server
    await server.start()
    logger.info("gRPC server is now running")

    # Graceful shutdown
    try:
        await server.wait_for_termination()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutdown signal received. Stopping server...")
        await server.stop(grace=None)  # graceful stop immediately


if __name__ == "__main__":
    try:
        asyncio.run(serve())
    except Exception as e:
        logger.error("gRPC server failed to start", error=str(e))
        raise
