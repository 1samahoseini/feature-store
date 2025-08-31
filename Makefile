.PHONY: help install up down logs test lint format clean seed-data deploy-gcp

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	pip install -r requirements.txt
	pip install -e .

up: ## Start local development stack
	docker-compose up -d
	@echo "✅ Stack started!"
	@echo "REST API: http://localhost:8000"
	@echo "gRPC API: localhost:50051"
	@echo "Airflow: http://localhost:8080 (admin/admin)"
	@echo "Grafana: http://localhost:3000 (admin/admin)"

down: ## Stop local development stack
	docker-compose down -v

logs: ## Show logs
	docker-compose logs -f

test: ## Run all tests
	pytest tests/ -v

test-unit: ## Run unit tests
	pytest tests/unit/ -v

test-integration: ## Run integration tests
	pytest tests/integration/ -v

test-performance: ## Run performance tests
	pytest tests/integration/test_performance.py -v

test-grpc: ## Run gRPC tests
	pytest tests/unit/test_grpc.py -v

test-migration: ## Test database migration
	python scripts/migrate_database.py --test

lint: ## Lint code
	black --check src/ tests/
	flake8 src/ tests/
	isort --check-only src/ tests/

format: ## Format code
	black src/ tests/
	isort src/ tests/

seed-data: ## Generate test data
	python scripts/seed_data.py --users=2000000

benchmark: ## Run benchmarks
	python scripts/benchmark.py

monitor: ## Open monitoring dashboards
	@echo "Opening Grafana dashboards..."
	open http://localhost:3000

clean: ## Clean up
	docker-compose down -v --remove-orphans
	docker system prune -f

# GCP Deployment
build-gcp: ## Build for GCP
	docker build -t gcr.io/$(GCP_PROJECT_ID)/feature-store:latest -f deployment/gcp/Dockerfile.gcp .

deploy-gcp: ## Deploy to GCP
	@echo "Deploying to GCP Cloud Run..."
	gcloud run deploy feature-store \
		--image gcr.io/$(GCP_PROJECT_ID)/feature-store:latest \
		--platform managed \
		--region $(GCP_REGION) \
		--allow-unauthenticated \
		--memory 2Gi \
		--cpu 2 \
		--min-instances 2 \
		--max-instances 50

generate-grpc: ## Generate gRPC code
	python -m grpc_tools.protoc \
		--proto_path=src/proto \
		--python_out=src/proto \
		--grpc_python_out=src/proto \
		src/proto/feature_store.proto