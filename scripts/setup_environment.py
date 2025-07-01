"""Environment setup script."""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from finops.config.settings import Settings
from finops.data.storage.file_storage import FileStorage
import asyncio


async def setup_storage_directories():
    """Setup storage directories."""
    settings = Settings.create_from_env()
    storage = FileStorage(settings.storage.base_path)
    
    # Create required collections
    collections = [
        "discovery_results",
        "cost_analysis", 
        "metrics",
        "clusters",
        "costs"
    ]
    
    for collection in collections:
        collection_path = storage.base_path / collection
        collection_path.mkdir(parents=True, exist_ok=True)
        print(f"✅ Created collection directory: {collection_path}")
    
    print(f"📁 Storage initialized at: {storage.base_path}")


def create_example_env_file():
    """Create example .env file."""
    env_content = """# Azure Configuration
AZURE_SUBSCRIPTION_ID=your-subscription-id
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret
AZURE_RESOURCE_GROUP=your-resource-group
AZURE_LOG_ANALYTICS_WORKSPACE_ID=your-workspace-id

# Kubernetes Configuration
K8S_KUBECONFIG_PATH=/path/to/kubeconfig
K8S_NAMESPACE=default

# Discovery Configuration
DISCOVERY_INTERVAL_HOURS=24
DISCOVERY_PARALLEL_WORKERS=5
DISCOVERY_TIMEOUT_SECONDS=300

# Storage Configuration
STORAGE_BACKEND=file
STORAGE_BASE_PATH=./data

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json
"""
    
    env_file = Path(".env.example")
    with open(env_file, 'w') as f:
        f.write(env_content)
    
    print(f"📝 Created example environment file: {env_file}")
    
    if not Path(".env").exists():
        print("💡 Copy .env.example to .env and update with your values")


def create_docker_compose():
    """Create docker-compose.yml for development."""
    docker_compose_content = """version: '3.8'

services:
  finops-discovery:
    build:
      context: .
      dockerfile: deployment/docker/Dockerfile
    environment:
      - ENVIRONMENT=development
    env_file:
      - .env
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    command: python scripts/discovery_runner.py
    restart: unless-stopped
    
  finops-api:
    build:
      context: .
      dockerfile: deployment/docker/Dockerfile
    environment:
      - ENVIRONMENT=development
    env_file:
      - .env
    ports:
      - "8000:8000"
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    command: python -m finops.cli serve --host 0.0.0.0 --port 8000
    restart: unless-stopped
    depends_on:
      - finops-discovery

volumes:
  finops-data:
  finops-logs:
"""
    
    compose_file = Path("docker-compose.yml")
    with open(compose_file, 'w') as f:
        f.write(docker_compose_content)
    
    print(f"🐳 Created Docker Compose file: {compose_file}")


def create_makefile():
    """Create Makefile for common tasks."""
    makefile_content = """# Kubernetes FinOps Platform Makefile

.PHONY: help install install-dev setup test lint format clean discover analyze serve docker-build docker-up docker-down

help:
	@echo "Available commands:"
	@echo "  install       Install production dependencies"
	@echo "  install-dev   Install development dependencies"
	@echo "  setup         Setup development environment"
	@echo "  test          Run tests"
	@echo "  lint          Run linting"
	@echo "  format        Format code"
	@echo "  clean         Clean build artifacts"
	@echo "  discover      Run discovery"
	@echo "  analyze       Run analysis"
	@echo "  serve         Start API server"
	@echo "  docker-build  Build Docker image"
	@echo "  docker-up     Start with Docker Compose"
	@echo "  docker-down   Stop Docker Compose"

install:
	pip install -e .[azure,kubernetes]

install-dev:
	pip install -e .[azure,kubernetes,dev,api]

setup: install-dev
	python scripts/setup_environment.py
	pre-commit install

test:
	pytest tests/ -v --cov=finops --cov-report=html

lint:
	flake8 src/ tests/
	mypy src/

format:
	black src/ tests/ scripts/
	isort src/ tests/ scripts/

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

discover:
	python -m finops.cli discover

analyze:
	python -m finops.cli analyze

serve:
	python -m finops.cli serve --reload

docker-build:
	docker build -f deployment/docker/Dockerfile -t finops:latest .

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down
"""
    
    makefile = Path("Makefile")
    with open(makefile, 'w') as f:
        f.write(makefile_content)
    
    print(f"🔧 Created Makefile: {makefile}")


async def main():
    """Main setup function."""
    print("🚀 Setting up Kubernetes FinOps Platform environment...")
    
    # Create storage directories
    await setup_storage_directories()
    
    # Create configuration files
    create_example_env_file()
    create_docker_compose()
    create_makefile()
    
    # Create logs directory
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    print(f"📝 Created logs directory: {logs_dir}")
    
    print("\n✅ Environment setup completed!")
    print("\n📋 Next steps:")
    print("1. Copy .env.example to .env and configure your Azure credentials")
    print("2. Install dependencies: make install-dev")
    print("3. Run discovery: make discover")
    print("4. Start API server: make serve")


if __name__ == "__main__":
    asyncio.run(main())