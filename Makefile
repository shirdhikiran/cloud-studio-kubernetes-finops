# Kubernetes FinOps Platform Makefile

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
