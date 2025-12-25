.PHONY: help dev-up dev-down dev-restart dev-logs install test lint format clean test-data test-data-download test-data-clone test-data-status deps version-check release-check nf-core-lint nf-core-diff nf-core-create-branch nf-core-update-branch nf-core-status

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	uv sync --dev

deps: ## Install system dependencies (bcftools)
	@if command -v brew >/dev/null 2>&1; then \
		brew bundle; \
	else \
		echo "Install bcftools manually: apt-get install bcftools or conda install -c bioconda bcftools"; \
	fi

dev-up: ## Start development database
	@if command -v docker >/dev/null 2>&1; then \
		if command -v docker-compose >/dev/null 2>&1; then \
			cd docker && docker-compose up -d; \
		elif docker compose version >/dev/null 2>&1; then \
			cd docker && docker compose up -d; \
		else \
			echo "Error: Neither 'docker-compose' nor 'docker compose' command found"; \
			exit 1; \
		fi; \
		echo "Waiting for PostgreSQL to be ready..."; \
		timeout 30 bash -c 'until docker exec vcf-pg-test pg_isready -U vcftest -d vcftest; do sleep 1; done'; \
		echo "PostgreSQL is ready!"; \
	else \
		echo "Error: Docker is not installed or not in PATH"; \
		echo "Please install Docker Desktop from https://www.docker.com/products/docker-desktop"; \
		exit 1; \
	fi

dev-down: ## Stop development database
	@if command -v docker >/dev/null 2>&1; then \
		if command -v docker-compose >/dev/null 2>&1; then \
			cd docker && docker-compose down; \
		elif docker compose version >/dev/null 2>&1; then \
			cd docker && docker compose down; \
		else \
			echo "Error: Neither 'docker-compose' nor 'docker compose' command found"; \
			exit 1; \
		fi; \
	else \
		echo "Error: Docker is not installed or not in PATH"; \
		exit 1; \
	fi

dev-restart: ## Restart development database
	@if command -v docker >/dev/null 2>&1; then \
		if command -v docker-compose >/dev/null 2>&1; then \
			cd docker && docker-compose restart; \
		elif docker compose version >/dev/null 2>&1; then \
			cd docker && docker compose restart; \
		else \
			echo "Error: Neither 'docker-compose' nor 'docker compose' command found"; \
			exit 1; \
		fi; \
	else \
		echo "Error: Docker is not installed or not in PATH"; \
		exit 1; \
	fi

dev-logs: ## Show development database logs
	@if command -v docker >/dev/null 2>&1; then \
		if command -v docker-compose >/dev/null 2>&1; then \
			cd docker && docker-compose logs -f postgres-test; \
		elif docker compose version >/dev/null 2>&1; then \
			cd docker && docker compose logs -f postgres-test; \
		else \
			echo "Error: Neither 'docker-compose' nor 'docker compose' command found"; \
			exit 1; \
		fi; \
	else \
		echo "Error: Docker is not installed or not in PATH"; \
		exit 1; \
	fi

test: ## Run tests
	pytest

test-cov: ## Run tests with coverage
	pytest --cov=src/vcf_pg_loader --cov-report=html --cov-report=term

lint: ## Run linting
	ruff check src tests

format: ## Format code
	ruff format src tests

clean: ## Clean up build artifacts and containers
	cd docker && docker-compose down -v
	rm -rf dist/ build/ *.egg-info/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

test-data: test-data-download ## Setup test data (alias for download)

test-data-download: ## Download nf-core test VCFs (~50MB)
	./scripts/setup_test_data.sh download

test-data-clone: ## Clone full nf-core/test-datasets repo (~2GB)
	./scripts/setup_test_data.sh clone

test-data-status: ## Show test data status
	./scripts/setup_test_data.sh status

test-data-clean: ## Remove cached test data
	./scripts/setup_test_data.sh clean

test-nf-core: test-data ## Run tests with nf-core data
	uv run pytest -v -m "nf_core"

test-integration: dev-up test-data ## Run integration tests
	uv run pytest -v -m "integration"

test-acceptance: test-data ## Run acceptance tests
	uv run pytest -v -m "acceptance"

version-check: ## Check all version references are in sync
	./scripts/check-version-sync.sh

release-check: ## Pre-flight validation for release (VERSION=X.Y.Z optional)
	@if [ -z "$(VERSION)" ]; then \
		./scripts/validate-release.sh; \
	else \
		./scripts/validate-release.sh $(VERSION); \
	fi

nf-core-lint: ## Lint local nf-core module
	./scripts/nf-core-module.sh lint

nf-core-diff: ## Show diff against upstream nf-core/modules
	./scripts/nf-core-module.sh diff

nf-core-create-branch: ## Create branch in fork (VERSION=X.Y.Z required)
	@if [ -z "$(VERSION)" ]; then \
		echo "Usage: make nf-core-create-branch VERSION=X.Y.Z"; \
		exit 1; \
	fi
	./scripts/nf-core-module.sh create-branch $(VERSION)

nf-core-update-branch: ## Update existing branch in fork (BRANCH=name required)
	@if [ -z "$(BRANCH)" ]; then \
		echo "Usage: make nf-core-update-branch BRANCH=update-vcfpgloader-X.Y.Z"; \
		exit 1; \
	fi
	./scripts/nf-core-module.sh update-branch $(BRANCH)

nf-core-status: ## Show nf-core fork/PR status
	./scripts/nf-core-module.sh status