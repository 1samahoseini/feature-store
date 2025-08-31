"""
Feature Store FastAPI Application
Main entrypoint for REST API server
"""

import time
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from prometheus_client import Counter, Histogram, generate_latest

from src.config.settings import get_settings
from src.feature_store.api import router as feature_router
from src.feature_store.database import init_db

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer()
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

# Prometheus metrics
REQUEST_COUNT = Counter(
    "feature_store_requests_total",
    "Total requests",
    ["method", "endpoint", "status"]
)

REQUEST_DURATION = Histogram(
    "feature_store_request_duration_seconds",
    "Request duration",
    ["method", "endpoint"]
)

CACHE_HITS = Counter(
    "feature_store_cache_hits_total",
    "Cache hits",
    ["hit_type"]
)

FEATURE_REQUESTS = Counter(
    "feature_store_feature_requests_total",
    "Feature requests",
    ["feature_type"]
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management"""
    logger.info("Starting Feature Store API")
    
    # Initialize database
    await init_db()
    
    logger.info("Feature Store API started successfully")
    yield
    
    logger.info("Shutting down Feature Store API")


# Initialize FastAPI app
app = FastAPI(
    title="BNPL Feature Store",
    description="Production-ready feature store for real-time BNPL credit decisioning",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    """Prometheus metrics middleware"""
    start_time = time.time()
    
    response = await call_next(request)
    
    duration = time.time() - start_time
    REQUEST_DURATION.labels(
        method=request.method,
        endpoint=request.url.path
    ).observe(duration)
    
    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=request.url.path,
        status=response.status_code
    ).inc()
    
    return response


@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    """Request logging middleware"""
    start_time = time.time()
    
    response = await call_next(request)
    
    duration_ms = round((time.time() - start_time) * 1000, 2)
    logger.info(
        "Request processed",
        method=request.method,
        path=request.url.path,
        status_code=response.status_code,
        duration_ms=duration_ms
    )
    
    return response


# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    settings = get_settings()
    return {
        "status": "healthy",
        "environment": settings.env,
        "version": "1.0.0",
        "timestamp": time.time()
    }


# Metrics endpoint
@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(generate_latest(), media_type="text/plain")


# Include feature store router
app.include_router(
    feature_router,
    prefix="/api/v1",
    tags=["features"]
)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    logger.error(
        "Unhandled exception",
        path=request.url.path,
        method=request.method,
        error=str(exc),
        exc_info=True
    )
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": "An unexpected error occurred"
        }
    )


if __name__ == "__main__":
    import uvicorn
    
    settings = get_settings()
    
    uvicorn.run(
        "src.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level="info" if not settings.debug else "debug"
    )
