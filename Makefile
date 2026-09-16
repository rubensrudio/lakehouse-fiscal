.DEFAULT_GOAL := help

ifneq (,$(wildcard ./.env))
include .env
export
endif

.PHONY: help lint format test-unit test-integration test bootstrap teardown seed seed-scale10 run-bronze run-silver run-gold run-all verify sql maintenance test-maintenance-scale10

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z0-9_-]+:.*## / {printf "%-28s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

lint: ## Run lint, formatting, and strict type checks
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy --strict src/

format: ## Format and auto-fix Python sources
	uv run ruff check --fix .
	uv run ruff format .

test-unit: ## Run unit tests without Docker
	uv run pytest tests/unit -q

test-integration: ## Run integration tests against the local stack
	./scripts/run_in_spark.sh pytest tests/integration -q -m integration

test: ## Run all tests with the coverage gate
	./scripts/run_in_spark.sh pytest tests -q --cov=src/lakehouse_fiscal --cov-fail-under=80 --cov-report=term --cov-report=json:coverage.json
	./scripts/run_in_spark.sh scripts.check_module_coverage coverage.json

bootstrap: ## Build and start Spark, MinIO, and PostgreSQL
	./scripts/bootstrap_local.sh

teardown: ## Stop the stack and remove its volumes
	./scripts/teardown_local.sh

seed: ## Seed the deterministic scale-1 ERP dataset
	uv run python -m lakehouse_fiscal.generator.postgres_seeder --seed 42 --scale 1

seed-scale10: ## Seed the scale-10 maintenance dataset
	uv run python -m lakehouse_fiscal.generator.postgres_seeder --seed 42 --scale 10

run-bronze: ## Run PostgreSQL batch ingestion
	./scripts/run_in_spark.sh lakehouse_fiscal.pipelines.run_bronze

run-silver: ## Run data quality and silver transformations
	./scripts/run_in_spark.sh lakehouse_fiscal.pipelines.run_silver

run-gold: ## Build dimensions, facts, and aggregates
	./scripts/run_in_spark.sh lakehouse_fiscal.pipelines.run_gold

run-all: ## Run the complete medallion pipeline
	./scripts/run_in_spark.sh lakehouse_fiscal.pipelines.run_all

verify: ## Verify reconciliation and SCD2 invariants
	./scripts/run_in_spark.sh lakehouse_fiscal.pipelines.run_all --verify-only

sql: ## Execute a SQL file (usage: make sql FILE=sql/analytics/...)
	@test -n "$(FILE)" || (echo "FILE is required" && exit 2)
	./scripts/run_in_spark.sh lakehouse_fiscal.pipelines.run_all --sql "$(FILE)"

maintenance: ## Apply supported table maintenance
	uv run python -m lakehouse_fiscal.pipelines.run_maintenance

test-maintenance-scale10: ## Run the isolated scale-10 maintenance test
	uv run pytest tests/integration -q -m scale10
