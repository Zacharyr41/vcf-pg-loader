# CLAUDE.md

## Project Overview
High-performance VCF-to-PostgreSQL loader with clinical-grade compliance.

## Commands
- `uv sync --extra dev` - Install dependencies
- `uv run pytest -v` - Run all tests
- `uv run ruff check src tests` - Lint code
- `make test` - Run tests via Makefile
- `make lint` - Run linting via Makefile

## Code Style
- Python 3.11+, use modern type hints (dict, list, | None instead of Dict, List, Optional)
- No comments unless explicitly requested
- Follow ruff linting rules

## Git Commits
- Keep commit messages concise and focused on the "why"

## Testing
- Use pytest with pytest-asyncio for async tests
- Integration tests use testcontainers for PostgreSQL
- Mark integration tests with @pytest.mark.integration
