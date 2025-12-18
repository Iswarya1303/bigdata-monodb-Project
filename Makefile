# Makefile for MongoDB Big Data Pipeline
.PHONY: install setup start stop clean test lint format dashboard pipeline generate-data setup-db help

# Default target
help:
	@echo "MongoDB Big Data Pipeline - Available Commands:"
	@echo ""
	@echo "  make install       - Install UV and project dependencies"
	@echo "  make setup         - Start Docker containers and initialize cluster"
	@echo "  make start         - Start Docker containers only"
	@echo "  make stop          - Stop Docker containers (keeps data)"
	@echo "  make clean         - Stop containers and remove all data/cache"
	@echo ""
	@echo "  make generate-data - Generate sample dataset (1M rows)"
	@echo "  make setup-db      - Setup database indexes and sharding"
	@echo "  make pipeline      - Run the complete data pipeline"
	@echo "  make dashboard     - Start Streamlit dashboard"
	@echo ""
	@echo "  make test          - Run pytest test suite"
	@echo "  make lint          - Run mypy and ruff checks"
	@echo "  make format        - Format code with black and ruff"
	@echo ""
	@echo "Quick Start:"
	@echo "  1. make install"
	@echo "  2. make setup"
	@echo "  3. make generate-data"
	@echo "  4. make pipeline"
	@echo "  5. make dashboard"

install:
	@echo "Installing UV package manager..."
	pip install uv
	@echo "Installing project dependencies..."
	uv sync
	@echo "Installation complete!"

setup:
	@echo "Starting MongoDB cluster..."
	docker compose -f docker/docker-compose.yml up -d
	@echo "Waiting for containers to initialize..."
	sleep 20
	@echo "Initializing sharding..."
	bash docker/setup-cluster.sh
	@echo "Cluster setup complete!"

start:
	@echo "Starting containers..."
	docker compose -f docker/docker-compose.yml up -d
	@echo "Containers started!"

stop:
	@echo "Stopping containers..."
	docker compose -f docker/docker-compose.yml down
	@echo "Containers stopped!"

clean:
	@echo "Stopping containers and removing volumes..."
	docker compose -f docker/docker-compose.yml down -v
	@echo "Cleaning Python cache..."
	rm -rf __pycache__ .pytest_cache .mypy_cache htmlcov .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -f *.log
	@echo "Cleanup complete!"

generate-data:
	@echo "Generating sample dataset..."
	uv run python scripts/generate_data.py
	@echo "Data generation complete!"

setup-db:
	@echo "Setting up database..."
	uv run python scripts/setup_database.py
	@echo "Database setup complete!"

pipeline:
	@echo "Running data pipeline..."
	uv run python scripts/run_pipeline.py

dashboard:
	@echo "Starting Streamlit dashboard..."
	@echo "Open http://localhost:8501 in your browser"
	uv run streamlit run dashboard/app.py

test:
	@echo "Running tests..."
	uv run pytest

lint:
	@echo "Running type checks..."
	uv run mypy src/
	@echo "Running linter..."
	uv run ruff check src/

format:
	@echo "Formatting code..."
	uv run black src/ tests/
	uv run ruff check --fix src/
	@echo "Formatting complete!"
