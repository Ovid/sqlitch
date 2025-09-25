# Makefile for sqlitch development

.PHONY: help install install-dev test test-unit test-integration test-compatibility lint format mypy coverage clean build docs docker-build docker-test release

# Default target
help:
	@echo "Available targets:"
	@echo "  install          Install sqlitch in development mode"
	@echo "  install-dev      Install with development dependencies"
	@echo "  test             Run all tests"
	@echo "  test-unit        Run unit tests only"
	@echo "  test-integration Run integration tests only"
	@echo "  test-compatibility Run compatibility tests against Perl sqitch"
	@echo "  test-code-quality Run code quality tests (Black, isort, syntax)"
	@echo "  lint             Run linting checks"
	@echo "  format           Format code with black and isort"
	@echo "  mypy             Run type checking"
	@echo "  vulture          Find unused code (high confidence)"
	@echo "  vulture-all      Find unused code (medium confidence)"
	@echo "  vulture-whitelist Generate whitelist for false positives"
	@echo "  coverage         Run tests with coverage reporting"
	@echo "  clean            Clean build artifacts and cache"
	@echo "  build            Build distribution packages"
	@echo "  docs             Build documentation"
	@echo "  docker-build     Build Docker images"
	@echo "  docker-test      Run tests in Docker"
	@echo "  release          Prepare for release"

# Installation targets
install:
	pip install -e .

install-dev:
	pip install -e .[dev]

# Testing targets
test: test-unit test-integration

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v

test-compatibility:
	pytest -m compatibility -v

test-all:
	pytest tests/ -v --cov=sqlitch --cov-report=html --cov-report=term-missing

test-code-quality:
	pytest tests/unit/test_code_quality.py -v --no-cov

# Code quality targets
lint:
	black --check --diff sqlitch tests
	isort --check-only --diff sqlitch tests
	flake8 sqlitch tests

format:
	black sqlitch tests
	isort sqlitch tests

mypy:
	mypy sqlitch

vulture:
	vulture sqlitch/ --min-confidence 80

vulture-all:
	vulture sqlitch/ --min-confidence 60

vulture-whitelist:
	vulture sqlitch/ --make-whitelist > vulture_generated_whitelist.py

# Coverage target
coverage:
	pytest tests/ --cov=sqlitch --cov-report=html --cov-report=xml --cov-report=term-missing --cov-fail-under=80

# Cleanup targets
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf coverage.xml
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Build targets
build: clean
	python -m build

# Documentation targets
docs:
	sphinx-build -b html docs docs/_build/html

docs-serve: docs
	python -m http.server 8000 --directory docs/_build/html

# Docker targets
docker-build:
	docker build -t sqlitch:latest .
	docker build -t sqlitch:dev --target development .

docker-test:
	docker-compose --profile test up --build --abort-on-container-exit

docker-lint:
	docker-compose --profile lint up --build --abort-on-container-exit

docker-docs:
	docker-compose --profile docs up --build

# Database testing targets
test-postgres:
	docker-compose up -d postgres
	sleep 10
	pytest tests/integration/test_pg_engine_integration.py -v
	docker-compose down

test-mysql:
	docker-compose up -d mysql
	sleep 15
	pytest tests/integration/test_mysql_engine_integration.py -v
	docker-compose down

test-oracle:
	docker-compose --profile oracle up -d oracle
	sleep 30
	pytest tests/integration/test_oracle_engine_integration.py -v
	docker-compose --profile oracle down

# Tox targets
tox:
	tox

tox-parallel:
	tox -p auto

# Pre-commit targets
pre-commit-install:
	pre-commit install

pre-commit-run:
	pre-commit run --all-files

# Release targets
release: clean lint mypy test-all build
	@echo "Release preparation complete!"
	@echo "Built packages:"
	@ls -la dist/
	@echo ""
	@echo "To publish to PyPI:"
	@echo "  twine upload dist/*"

release-test: clean lint mypy test-all build
	twine upload --repository testpypi dist/*

# Benchmark targets
benchmark:
	pytest tests/ -k "benchmark or performance" --benchmark-json=benchmark.json

# Security targets
security:
	bandit -r sqlitch
	safety check

# Development environment setup
dev-setup: install-dev pre-commit-install
	@echo "Development environment setup complete!"

# Quick development cycle
dev: format lint mypy test-unit
	@echo "Development cycle complete!"

# CI simulation
ci: lint mypy test-all coverage security
	@echo "CI simulation complete!"