"""
FastAPI REST API Routes
RESTful endpoints for feature serving
"""

import time
from typing import List

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
import structlog

from src.feature_store.models import (
    FeatureRequest, FeatureResponse, BatchFeatureRequest, BatchFeatureResponse,
    FeatureType, HealthStatus
)
from src.feature_store.store import feature_store
from src.feature_store.cache import cache
from src.feature_store.database import db

logger = structlog.get_logger(__name__)

router = APIRouter()


@router.get("/features/user/{user_id}", response_model=FeatureResponse)
async def get_user_features(
    user_id: str,
    feature_types: List[FeatureType] = Query(default=[FeatureType.USER]),
    include_metadata: bool = Query(default=False)
):
    """
    Get features for a specific user
    
    - **user_id**: User identifier
    - **feature_types**: List of feature types to retrieve (user, transaction, risk)
    - **include_metadata**: Include metadata about cache hits and freshness
    """
    time.time()
    
    try:
        # Validate user_id
        if not user_id or len(user_id) > 100:
            raise HTTPException(
                status_code=400,
                detail="Invalid user_id: must be non-empty and less than 100 characters"
            )
        
        # Create request
        request = FeatureRequest(
            user_id=user_id,
            feature_types=feature_types,
            include_metadata=include_metadata
        )
        
        # Get features
        response = await feature_store.get_features(request)
        
        logger.info(
            "Feature request processed",
            user_id=user_id,
            feature_types=[ft.value for ft in feature_types],
            response_time_ms=response.response_time_ms,
            cache_hit=response.cache_hit
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing feature request: {e}", user_id=user_id)
        raise HTTPException(
            status_code=500,
            detail="Internal server error while processing feature request"
        )


@router.post("/features/batch", response_model=BatchFeatureResponse)
async def get_batch_features(request: BatchFeatureRequest):
    """
    Get features for multiple users in a single request
    
    - **requests**: List of feature requests (max 100 users per batch)
    """
    time.time()
    
    try:
        # Validate batch size
        if len(request.requests) > 100:
            raise HTTPException(
                status_code=400,
                detail="Batch size too large: maximum 100 requests per batch"
            )
        
        # Process batch request
        response = await feature_store.get_batch_features(request)
        
        logger.info(
            "Batch feature request processed",
            total_requests=response.total_requests,
            successful_requests=response.successful_requests,
            failed_requests=response.failed_requests,
            cache_hit_ratio=response.cache_hit_ratio,
            total_response_time_ms=response.total_response_time_ms
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing batch feature request: {e}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error while processing batch request"
        )


@router.delete("/features/user/{user_id}")
async def invalidate_user_cache(user_id: str):
    """
    Invalidate cached features for a specific user
    
    - **user_id**: User identifier
    """
    try:
        success = await cache.delete_user_features(user_id)
        
        if success:
            logger.info(f"Cache invalidated for user {user_id}")
            return {"message": f"Cache invalidated for user {user_id}"}
        else:
            raise HTTPException(
                status_code=500,
                detail="Failed to invalidate cache"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error invalidating cache: {e}", user_id=user_id)
        raise HTTPException(
            status_code=500,
            detail="Internal server error while invalidating cache"
        )


@router.get("/features/health", response_model=HealthStatus)
async def feature_store_health():
    """
    Get feature store health status
    """
    try:
        # Check component health
        cache_healthy = await cache.health_check()
        db_healthy = await db.health_check()
        
        overall_healthy = cache_healthy and db_healthy
        
        health_status = HealthStatus(
            status="healthy" if overall_healthy else "unhealthy",
            environment="local",  # Will be overridden by settings
            version="1.0.0",
            timestamp=time.time(),
            checks={
                "cache": cache_healthy,
                "database": db_healthy,
                "overall": overall_healthy
            }
        )
        
        status_code = 200 if overall_healthy else 503
        
        return JSONResponse(
            status_code=status_code,
            content=health_status.dict()
        )
        
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": "Health check failed",
                "timestamp": time.time()
            }
        )


@router.get("/features/stats")
async def get_feature_store_stats():
    """
    Get feature store statistics and metrics
    """
    try:
        cache_stats = await cache.get_cache_stats()
        db_stats = await db.get_database_stats()
        
        return {
            "cache": cache_stats,
            "database": db_stats,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve statistics"
        )


@router.get("/features/user/{user_id}/history")
async def get_user_feature_history(user_id: str, days: int = Query(default=30, ge=1, le=365)):
    """
    Get feature history for a user (placeholder for future implementation)
    
    - **user_id**: User identifier
    - **days**: Number of days of history to retrieve
    """
    # This is a placeholder for future feature versioning/history functionality
    return {
        "user_id": user_id,
        "days": days,
        "message": "Feature history not yet implemented",
        "available_in": "v2.0.0"
    }


@router.post("/features/refresh")
async def refresh_features(user_ids: List[str]):
    """
    Trigger feature refresh for specific users
    
    - **user_ids**: List of user identifiers to refresh
    """
    try:
        if len(user_ids) > 1000:
            raise HTTPException(
                status_code=400,
                detail="Too many user IDs: maximum 1000 per request"
            )
        
        # Invalidate cache for all specified users
        invalidated_count = 0
        for user_id in user_ids:
            success = await cache.delete_user_features(user_id)
            if success:
                invalidated_count += 1
        
        logger.info(f"Refreshed features for {invalidated_count} users")
        
        return {
            "message": f"Feature refresh triggered for {invalidated_count} users",
            "requested": len(user_ids),
            "successful": invalidated_count
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error refreshing features: {e}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error while refreshing features"
        )


# Error handlers
# NOTE: Exception handlers must be registered on the FastAPI app, not the router.
# Move these handlers to your main application file and register them with `app.exception_handler`.

# Example to add in your main.py (or where FastAPI app is created):
#
# from fastapi import FastAPI, Request
# from fastapi.responses import JSONResponse
# from src.feature_store.models import ErrorResponse
#
# app = FastAPI()
#
# @app.exception_handler(ValueError)
# async def value_error_handler(request: Request, exc: ValueError):
#     return JSONResponse(
#         status_code=400,
#         content=ErrorResponse(
#             error="Validation Error",
#             message=str(exc)
#         ).dict()
#     )
#
# @app.exception_handler(404)
# async def not_found_handler(request: Request, exc):
#     return JSONResponse(
#         status_code=404,
#         content=ErrorResponse(
#             error="Not Found",
#             message="The requested resource was not found"
#         ).dict()
#     )