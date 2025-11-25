.PHONY: help dev-up dev-down dev-restart dev-logs install test lint format clean

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	uv sync --dev

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