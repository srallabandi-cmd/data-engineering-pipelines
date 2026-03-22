# ===========================================================================
# Data Engineering Pipelines - Makefile
# ===========================================================================

.PHONY: help setup test lint format spark-submit dbt-run dbt-test dbt-docs \
        docker-up docker-down docker-logs kafka-topics clean

# Default target
.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Variables
# ---------------------------------------------------------------------------
PYTHON        ?= python3
PIP           ?= pip3
SPARK_SUBMIT  ?= spark-submit
SPARK_MASTER  ?= spark://localhost:7077
DBT_DIR       ?= dbt
DBT_PROFILES  ?= dbt
DBT_TARGET    ?= dev
KAFKA_BROKER  ?= localhost:9092

SPARK_PACKAGES = io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
SPARK_CONF     = --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
                 --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
                 --conf spark.sql.adaptive.enabled=true

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------
help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
setup: ## Install Python dependencies and initialize the project
	$(PIP) install --upgrade pip
	$(PIP) install \
		pyspark==3.5.0 \
		delta-spark==3.1.0 \
		confluent-kafka==2.3.0 \
		great-expectations==0.18.8 \
		requests==2.31.0 \
		dbt-core==1.7.4 \
		dbt-postgres==1.7.4 \
		dbt-snowflake==1.7.1 \
		flake8==7.0.0 \
		black==24.1.1 \
		isort==5.13.2 \
		pytest==8.0.0 \
		pytest-cov==4.1.0 \
		pytest-mock==3.12.0
	@echo "Setup complete."

setup-dev: setup ## Setup for development with additional tools
	$(PIP) install \
		pre-commit==3.6.0 \
		ipython==8.20.0 \
		jupyter==1.0.0
	pre-commit install || true
	@echo "Dev setup complete."

# ---------------------------------------------------------------------------
# Testing
# ---------------------------------------------------------------------------
test: ## Run all tests with pytest
	$(PYTHON) -m pytest tests/ \
		-v \
		--tb=short \
		--cov=spark-jobs \
		--cov=streaming \
		--cov-report=term-missing \
		--cov-report=html:coverage_html \
		--junitxml=test-results.xml
	@echo "Tests complete. Coverage report: coverage_html/index.html"

test-unit: ## Run unit tests only
	$(PYTHON) -m pytest tests/unit/ -v --tb=short

test-integration: ## Run integration tests only
	$(PYTHON) -m pytest tests/integration/ -v --tb=short -m integration

# ---------------------------------------------------------------------------
# Linting & Formatting
# ---------------------------------------------------------------------------
lint: ## Run all linters (flake8, black check, isort check)
	@echo "=== flake8 ==="
	$(PYTHON) -m flake8 spark-jobs/ streaming/ airflow/ \
		--max-line-length=120 \
		--ignore=E501,W503,E203 \
		--exclude=__pycache__,.git,target
	@echo "=== black (check) ==="
	$(PYTHON) -m black --check --diff --line-length=120 \
		spark-jobs/ streaming/ airflow/
	@echo "=== isort (check) ==="
	$(PYTHON) -m isort --check-only --diff --profile=black \
		spark-jobs/ streaming/ airflow/
	@echo "All lint checks passed."

format: ## Auto-format code with black and isort
	$(PYTHON) -m black --line-length=120 spark-jobs/ streaming/ airflow/
	$(PYTHON) -m isort --profile=black spark-jobs/ streaming/ airflow/
	@echo "Formatting complete."

# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------
spark-submit: ## Submit a Spark job (usage: make spark-submit JOB=path/to/job.py ARGS="--flag value")
ifndef JOB
	$(error JOB is required. Usage: make spark-submit JOB=spark-jobs/etl/extract_api_data.py)
endif
	$(SPARK_SUBMIT) \
		--master $(SPARK_MASTER) \
		--packages $(SPARK_PACKAGES) \
		$(SPARK_CONF) \
		--py-files spark-jobs/utils/spark_session.py,spark-jobs/utils/data_quality.py \
		$(JOB) $(ARGS)

spark-submit-local: ## Submit a Spark job in local mode
ifndef JOB
	$(error JOB is required.)
endif
	$(SPARK_SUBMIT) \
		--master "local[*]" \
		--packages $(SPARK_PACKAGES) \
		$(SPARK_CONF) \
		--py-files spark-jobs/utils/spark_session.py,spark-jobs/utils/data_quality.py \
		$(JOB) $(ARGS)

# ---------------------------------------------------------------------------
# dbt
# ---------------------------------------------------------------------------
dbt-run: ## Run dbt models
	cd $(DBT_DIR) && dbt run --profiles-dir $(DBT_PROFILES) --target $(DBT_TARGET)

dbt-test: ## Run dbt tests
	cd $(DBT_DIR) && dbt test --profiles-dir $(DBT_PROFILES) --target $(DBT_TARGET)

dbt-docs: ## Generate and serve dbt documentation
	cd $(DBT_DIR) && dbt docs generate --profiles-dir $(DBT_PROFILES) --target $(DBT_TARGET)
	@echo "Run 'cd $(DBT_DIR) && dbt docs serve' to view documentation."

dbt-compile: ## Compile dbt models (no execution)
	cd $(DBT_DIR) && dbt compile --profiles-dir $(DBT_PROFILES) --target $(DBT_TARGET)

dbt-snapshot: ## Run dbt snapshots
	cd $(DBT_DIR) && dbt snapshot --profiles-dir $(DBT_PROFILES) --target $(DBT_TARGET)

dbt-clean: ## Clean dbt artifacts
	cd $(DBT_DIR) && dbt clean

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------
docker-up: ## Start the full local development stack
	docker compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@echo "Stack is up. Services:"
	@echo "  Kafka:           localhost:9092"
	@echo "  Schema Registry: localhost:8081"
	@echo "  Kafka UI:        http://localhost:8082"
	@echo "  Spark Master:    http://localhost:8083"
	@echo "  PostgreSQL:      localhost:5432"
	@echo "  MinIO:           http://localhost:9001 (admin: minioadmin/minioadmin)"

docker-down: ## Stop and remove all containers
	docker compose down -v --remove-orphans

docker-logs: ## Tail logs for all services
	docker compose logs -f --tail=100

docker-ps: ## Show running containers
	docker compose ps

docker-restart: docker-down docker-up ## Restart all services

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
kafka-topics: ## Create Kafka topics
	@echo "Creating Kafka topics..."
	docker compose exec kafka kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--topic events \
		--partitions 6 \
		--replication-factor 1 \
		--if-not-exists \
		--config retention.ms=604800000 \
		--config cleanup.policy=delete
	docker compose exec kafka kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--topic events-dead-letter \
		--partitions 3 \
		--replication-factor 1 \
		--if-not-exists \
		--config retention.ms=2592000000
	docker compose exec kafka kafka-topics --create \
		--bootstrap-server localhost:9092 \
		--topic user-profiles \
		--partitions 6 \
		--replication-factor 1 \
		--if-not-exists \
		--config cleanup.policy=compact
	@echo "Topics created."

kafka-list-topics: ## List all Kafka topics
	docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

kafka-describe: ## Describe a Kafka topic (usage: make kafka-describe TOPIC=events)
ifndef TOPIC
	$(error TOPIC is required. Usage: make kafka-describe TOPIC=events)
endif
	docker compose exec kafka kafka-topics --describe \
		--bootstrap-server localhost:9092 --topic $(TOPIC)

# ---------------------------------------------------------------------------
# Airflow
# ---------------------------------------------------------------------------
airflow-up: ## Start Airflow local environment
	cd airflow && docker compose up -d
	@echo "Airflow UI: http://localhost:8080 (admin/admin)"

airflow-down: ## Stop Airflow
	cd airflow && docker compose down -v

# ---------------------------------------------------------------------------
# Quality
# ---------------------------------------------------------------------------
quality-check: ## Run Great Expectations checkpoint
	cd data-quality && great_expectations checkpoint run daily_checkpoint

# ---------------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------------
clean: ## Remove build artifacts, caches, and temporary files
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf dist/ build/ coverage_html/ .coverage test-results.xml
	rm -rf spark-warehouse/ metastore_db/ derby.log
	rm -rf $(DBT_DIR)/target/ $(DBT_DIR)/dbt_packages/ $(DBT_DIR)/logs/
	@echo "Clean complete."
