# BNPL Feature Store - Deployment Guide

## Overview

This guide covers local development setup and GCP production deployment for the BNPL Feature Store. The system is designed for easy local development with one-command setup and seamless cloud deployment.

## Local Development Setup

### Prerequisites

**Required Software:**
```bash
# Check versions
python --version    # 3.11+
docker --version    # 20.0+
make --version      # 4.0+
git --version       # 2.30+Installation (Ubuntu/Debian):# Python 3.11+
sudo apt update && sudo apt install python3.11 python3.11-venv python3-pip

# Docker
curl -fsSL https://get.docker.com -o get-docker.sh && sh get-docker.sh
sudo usermod -aG docker $USER

# Docker Compose
sudo apt install docker-compose-plugin

# Make
sudo apt install build-essentialQuick Start1. Clone and Setup:git clone <repository-url>
cd feature-store

# Install Python dependencies
make install

# Start entire stack
make up2. Verify Services:# Check service health
make health

# Expected output:
# ✅ REST API: http://localhost:8000 - healthy
# ✅ gRPC API: localhost:50051 - healthy  
# ✅ Redis: localhost:6379 - healthy
# ✅ PostgreSQL: localhost:5432 - healthy
# ✅ Airflow: http://localhost:8080 - healthy
# ✅ Grafana: http://localhost:3000 - healthy3. Generate Test Data:# Create realistic test data (2M users)
make seed-data

# Quick test
curl "http://localhost:8000/api/v1/features/user/user_000001"Local Development WorkflowDaily Development:# Start services
make up

# Run tests during development  
make test-watch      # Auto-run tests on file changes

# Code quality checks
make lint && make format

# Stop services
make downAvailable Services: | Service | URL | Credentials | |---------|-----|-------------| | REST API | http://localhost:8000 | None | | gRPC API | localhost:50051 | None | | Airflow UI | http://localhost:8080 | admin/admin | | Grafana | http://localhost:3000 | admin/admin | | Redis Insight | http://localhost:8001 | None |Environment ConfigurationLocal Configuration (.env.local):# Database
DATABASE_URL=postgresql://postgres:password@localhost:5432/feature_store
REDIS_URL=redis://localhost:6379/0

# BigQuery (Emulator)
BIGQUERY_EMULATOR_HOST=localhost:9050
GOOGLE_CLOUD_PROJECT=local-project

# Feature Store
CACHE_TTL_SECONDS=7200
BATCH_SIZE_LIMIT=1000
MAX_CONCURRENT_REQUESTS=100

# Development
ENV=local
DEBUG=true
LOG_LEVEL=INFODocker Compose ArchitectureServices Overview:# docker-compose.yml structure
services:
  api:              # FastAPI + gRPC server
  redis:            # Cache layer
  postgres:         # Transactional database
  bigquery-emulator: # Local BigQuery
  kafka:            # Event streaming
  airflow-scheduler: # Batch processing
  airflow-worker:   # Job execution
  prometheus:       # Metrics collection
  grafana:          # Monitoring dashboardsGCP Production DeploymentPrerequisitesGCP Setup:# Install gcloud CLI
curl https://sdk.cloud.google.com | bash
source ~/.bashrc

# Authenticate
gcloud auth login
gcloud config set project your-project-id

# Enable required APIs
gcloud services enable \
  cloudbuild.googleapis.com \
  run.googleapis.com \
  sql.googleapis.com \
  memorystore.googleapis.com \
  bigquery.googleapis.comService Account Setup:# Create service account
gcloud iam service-accounts create feature-store-sa \
  --display-name="Feature Store Service Account"

# Grant permissions
gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:feature-store-sa@your-project-id.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:feature-store-sa@your-project-id.iam.gserviceaccount.com" \
  --role="roles/cloudsql.client"

# Create key
gcloud iam service-accounts keys create gcp-key.json \
  --iam-account=feature-store-sa@your-project-id.iam.gserviceaccount.comInfrastructure Setup1. Create GCP Resources:# PostgreSQL instance
gcloud sql instances create feature-store-db \
  --database-version=POSTGRES_15 \
  --tier=db-f1-micro \
  --region=us-central1

# Redis instance  
gcloud redis instances create feature-store-cache \
  --size=1 \
  --region=us-central1 \
  --redis-version=redis_7_0

# BigQuery dataset
bq mk --dataset your-project-id:feature_store2. Configure Environment (.env.gcp):# Database
DATABASE_URL=postgresql://postgres:password@/feature_store?host=/cloudsql/your-project-id:us-central1:feature-store-db
REDIS_URL=redis://10.0.0.3:6379/0

# BigQuery
GOOGLE_CLOUD_PROJECT=your-project-id
BIGQUERY_DATASET=feature_store

# Cloud Run
ENV=gcp
DEBUG=false
LOG_LEVEL=WARNINGDeployment OptionsOption 1: One-Command Deployment# Deploy to staging
ENV=gcp make deploy-staging

# Deploy to production (after staging validation)
ENV=gcp make deploy-productionOption 2: GitLab CI/CD (Recommended)# Push to main branch triggers automatic deployment
git push origin main

# Pipeline stages:
# 1. test        - Run all tests
# 2. build       - Build Docker image  
# 3. deploy-staging - Deploy to staging environment
# 4. deploy-production - Manual production deploymentOption 3: Manual Cloud Build# Build image
gcloud builds submit --tag gcr.io/your-project-id/feature-store:latest

# Deploy to Cloud Run
gcloud run deploy feature-store \
  --image gcr.io/your-project-id/feature-store:latest \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars ENV=gcp \
  --min-instances 1 \
  --max-instances 10 \
  --memory 2Gi \
  --cpu 2Production ConfigurationCloud Run Service Configuration:# service.yaml
apiVersion: serving.knative.dev/v1
kind: Service
metadata:
  name: feature-store
  annotations:
    run.googleapis.com/ingress: all
spec:
  template:
    metadata:
      annotations:
        autoscaling.knative.dev/minScale: "2"
        autoscaling.knative.dev/maxScale: "50"
        run.googleapis.com/cpu-throttling: "false"
        run.googleapis.com/memory: "2Gi"
        run.googleapis.com/cpu: "2"
    spec:
      serviceAccountName: feature-store-sa@your-project-id.iam.gserviceaccount.com
      containers:
      - image: gcr.io/your-project-id/feature-store:latest
        ports:
        - containerPort: 8000
        env:
        - name: ENV
          value: "gcp"
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: database-url
              key: url
        resources:
          limits:
            memory: "2Gi"
            cpu: "2000m"Health Checks:# Add to Cloud Run service
readinessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 10
  timeoutSeconds: 5
  periodSeconds: 10

livenessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 30
  timeoutSeconds: 10
  periodSeconds: 30Database MigrationPostgreSQL → CockroachDB Migration:# 1. Setup CockroachDB cluster
gcloud sql instances create feature-store-crdb \
  --database-version=COCKROACHDB \
  --tier=db-n1-standard-1 \
  --region=us-central1

# 2. Run migration
make migrate-to-cockroachdb

# 3. Validate migration
make test-migration

# 4. Switch traffic gradually
# Update DATABASE_URL in productionDeployment ValidationPost-Deployment Checks:# Health check
curl https://feature-store-prod-your-project.a.run.app/health

# Performance test
make test-production-performance

# Feature validation
curl "https://feature-store-prod-your-project.a.run.app/api/v1/features/user/user_000001"Expected Results:{
  "status": "healthy",
  "version": "1.0.0",
  "environment": "production",
  "components": {
    "database": "healthy",
    "cache": "healthy",
    "bigquery": "healthy"
  }
}Monitoring & Alerting SetupProduction Monitoring:# Setup monitoring
make setup-monitoring

# Configure alerts
make setup-alerts

# View dashboards
echo "Grafana: https://grafana-your-project.a.run.app"
echo "Prometheus: https://prometheus-your-project.a.run.app"TroubleshootingCommon Local IssuesPort Conflicts:# Check port usage
lsof -i :8000
lsof -i :6379

# Stop conflicting services
sudo systemctl stop redis
sudo systemctl stop postgresqlDocker Issues:# Clean docker state
make clean
docker system prune -a

# Rebuild from scratch
make build && make upDatabase Connection Issues:# Check PostgreSQL logs
docker logs feature-store_postgres_1

# Manual connection test
docker exec -it feature-store_postgres_1 psql -U postgres -d feature_storeCommon Production IssuesService Startup Failures:# Check Cloud Run logs
gcloud logging read "resource.type=cloud_run_revision" --limit=50

# Check service configuration
gcloud run services describe feature-store --region=us-central1Database Connection Issues:# Test Cloud SQL connection
gcloud sql connect feature-store-db --user=postgres

# Check connection limits
gcloud sql instances describe feature-store-dbPerformance Issues:# Check Cloud Run metrics
gcloud monitoring metrics list --filter="resource.type=cloud_run_revision"

# Scale up if needed
gcloud run services update feature-store \
  --max-instances 100 \
  --region us-central1Security ConsiderationsLocal DevelopmentDefault passwords in .env.local (development only)No TLS required for local servicesOpen ports on localhost onlyProductionService account authentication for all GCP servicesVPC networking for database connectionsTLS termination at load balancerSecret management via Google Secret ManagerBest Practices# Never commit secrets
echo "*.env.gcp" >> .gitignore
echo "gcp-key.json" >> .gitignore

# Use secret management
gcloud secrets create database-url --data-file=database-url.txt

