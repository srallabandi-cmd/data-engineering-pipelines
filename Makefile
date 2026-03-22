.PHONY: help setup test lint run-etl run-dbt run-streaming clean

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Set up local development environment
	docker-compose up -d
	pip install -r requirements.txt
	cd dbt && dbt deps

test: ## Run all tests
	pytest tests/ -v --tb=short
	cd dbt && dbt test

lint: ## Run linters
	flake8 spark-jobs/ streaming/
	black --check spark-jobs/ streaming/
	cd dbt && dbt compile

run-etl: ## Run ETL pipeline locally
	spark-submit --packages io.delta:delta-core_2.12:2.4.0 spark-jobs/etl/extract_api_data.py --api-url https://api.example.com/v1/events --output-path /tmp/data-lake/raw/events
	spark-submit --packages io.delta:delta-core_2.12:2.4.0 spark-jobs/etl/transform_events.py --input-path /tmp/data-lake/raw/events --output-path /tmp/data-lake/transformed/events
	spark-submit --packages io.delta:delta-core_2.12:2.4.0 spark-jobs/etl/load_warehouse.py --input-path /tmp/data-lake/transformed/events --target-path /tmp/data-warehouse/events

run-dbt: ## Run dbt transformations
	cd dbt && dbt run && dbt test

run-streaming: ## Start streaming pipeline
	spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-core_2.12:2.4.0 streaming/spark_streaming_consumer.py

clean: ## Clean up local environment
	docker-compose down -v
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	cd dbt && dbt clean
