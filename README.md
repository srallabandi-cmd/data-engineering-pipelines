# Data Engineering Pipelines

[![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Airflow](https://img.shields.io/badge/Airflow-2.8-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Kafka](https://img.shields.io/badge/Kafka-3.6-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Great Expectations](https://img.shields.io/badge/Great_Expectations-0.18-FF6310)](https://greatexpectations.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Production-grade data engineering platform featuring batch and streaming ETL pipelines, data quality validation, dimensional modeling, and ML workflow orchestration.

## Architecture

```mermaid
flowchart LR
    subgraph Sources
        API[REST APIs]
        DB[(OLTP Databases)]
        S3[Cloud Storage]
        IOT[IoT Devices]
    end

    subgraph Ingestion
        KP[Kafka Producer]
        SR[Schema Registry]
        KT[Kafka Topics]
    end

    subgraph Processing
        SSC[Spark Structured Streaming]
        SE[Spark Batch ETL]
        DQ[Data Quality Checks]
    end

    subgraph Storage
        DL[(Delta Lake)]
        STG[Staging Layer]
        PROC[Processed Layer]
    end

    subgraph Transformation
        DBT_STG[dbt Staging Models]
        DBT_INT[dbt Intermediate Models]
        DBT_MART[dbt Marts]
    end

    subgraph Serving
        DW[(Data Warehouse)]
        DASH[Dashboards]
        ML[ML Models]
    end

    subgraph Orchestration
        AF[Airflow DAGs]
        MON[Monitoring & Alerts]
    end

    API --> KP
    DB --> KP
    S3 --> SE
    IOT --> KP
    KP --> SR
    KP --> KT
    KT --> SSC
    SSC --> DL
    SE --> STG
    STG --> DQ
    DQ --> PROC
    PROC --> DBT_STG
    DL --> DBT_STG
    DBT_STG --> DBT_INT
    DBT_INT --> DBT_MART
    DBT_MART --> DW
    DW --> DASH
    DW --> ML
    AF --> SE
    AF --> DQ
    AF --> DBT_STG
    AF --> MON
```

## Features

- **Batch ETL**: PySpark jobs with API extraction, complex transformations, and warehouse loading
- **Streaming**: Kafka producers, Spark Structured Streaming consumers with exactly-once semantics
- **Schema Management**: Confluent Schema Registry with schema evolution (BACKWARD, FORWARD, FULL)
- **Data Quality**: Great Expectations suites, custom PySpark validation framework, freshness and anomaly checks
- **Orchestration**: Airflow DAGs for daily ETL, data quality, and ML pipelines with Slack alerting
- **Dimensional Modeling**: dbt models across staging, intermediate, and marts layers with SCD Type 2
- **CI/CD**: GitHub Actions for linting, testing, dbt compilation, and Docker build verification
- **Local Development**: Full Docker Compose stack with Kafka, Spark, PostgreSQL, MinIO, and Airflow

## Prerequisites

- Docker and Docker Compose v2.20+
- Python 3.10+
- Java 11 (for local Spark)
- GNU Make

## Quick Start

```bash
# Clone the repository
git clone https://github.com/<your-org>/data-engineering-pipelines.git
cd data-engineering-pipelines

# Start the full local stack
make setup
make docker-up

# Create Kafka topics
make kafka-topics

# Run the batch ETL pipeline
make spark-submit JOB=spark-jobs/etl/extract_api_data.py

# Run dbt models
make dbt-run

# Run tests
make test

# Tear down
make docker-down
```

## Project Structure

```
data-engineering-pipelines/
├── spark-jobs/
│   ├── etl/
│   │   ├── extract_api_data.py       # API extraction with retry logic
│   │   ├── transform_events.py       # Complex PySpark transformations
│   │   └── load_warehouse.py         # Warehouse loading with upsert/SCD
│   └── utils/
│       ├── spark_session.py           # Spark session builder
│       └── data_quality.py            # Custom DQ framework
├── airflow/
│   ├── dags/
│   │   ├── daily_etl_pipeline.py      # Main ETL orchestration
│   │   ├── data_quality_checks.py     # Post-ETL quality checks
│   │   └── ml_pipeline_dag.py         # ML training and deployment
│   ├── plugins/
│   │   └── custom_operators.py        # Custom Airflow operators
│   └── docker-compose.yml             # Local Airflow environment
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_events.sql
│   │   │   ├── stg_users.sql
│   │   │   └── schema.yml
│   │   ├── intermediate/
│   │   │   ├── int_user_sessions.sql
│   │   │   └── schema.yml
│   │   └── marts/
│   │       ├── dim_users.sql
│   │       ├── fct_events.sql
│   │       └── schema.yml
│   └── macros/
│       ├── generate_schema_name.sql
│       └── test_macros.sql
├── data-quality/
│   └── great_expectations/
│       ├── expectations/
│       │   └── events_suite.json
│       └── checkpoints/
│           └── daily_checkpoint.yml
├── streaming/
│   ├── kafka_producer.py
│   ├── spark_streaming_consumer.py
│   └── schema_registry.py
├── .github/
│   └── workflows/
│       └── test-pipelines.yml
├── docker-compose.yml                 # Full local dev stack
├── Makefile
├── .gitignore
└── README.md
```

## Components

### Spark ETL Jobs

The `spark-jobs/` directory contains PySpark applications for batch and streaming workloads:

- **extract_api_data.py** - Extracts data from REST APIs with exponential backoff retry, pagination, rate limiting, and schema enforcement. Writes to the staging layer as Parquet.
- **transform_events.py** - Applies complex transformations including window functions (lag, lead, row_number), session identification, pivot operations, UDFs, and data enrichment via joins.
- **load_warehouse.py** - Loads processed data into the warehouse using Delta MERGE for upserts, SCD Type 2 dimension handling, and post-load validation with checksums.

### Airflow DAGs

- **daily_etl_pipeline.py** - Full orchestration: sensor for data arrival, extraction, Spark transformations, dbt run, quality checks, Slack notifications.
- **data_quality_checks.py** - Post-ETL validation: Great Expectations checkpoints, SQL checks, freshness, volume anomalies.
- **ml_pipeline_dag.py** - ML workflow: feature computation, training, evaluation, conditional deployment via BranchPythonOperator.

### dbt Models

Three-layer architecture following the dbt best practices:

| Layer | Purpose | Materialization |
|-------|---------|-----------------|
| **Staging** | Source cleaning, type casting, dedup | Incremental |
| **Intermediate** | Business logic, sessionization | Ephemeral / Table |
| **Marts** | Dimensions and facts for analytics | Table |

### Streaming

- **kafka_producer.py** - Produces events to Kafka with JSON schema validation, delivery guarantees, and key-based partitioning.
- **spark_streaming_consumer.py** - Consumes from Kafka with watermarking, window aggregations, and Delta Lake output.
- **schema_registry.py** - Manages Avro/JSON schemas with compatibility enforcement.

## Configuration

All credentials and environment-specific configuration are managed via environment variables. See `.env.example` for the full list. Never commit secrets.

| Variable | Description |
|----------|-------------|
| `SPARK_MASTER` | Spark cluster URL |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses |
| `WAREHOUSE_JDBC_URL` | Data warehouse JDBC connection |
| `S3_ENDPOINT` | S3-compatible storage endpoint |
| `AIRFLOW__CORE__EXECUTOR` | Airflow executor type |

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Write tests for new functionality
4. Ensure all quality checks pass (`make lint && make test`)
5. Commit your changes following [Conventional Commits](https://www.conventionalcommits.org/)
6. Open a pull request with a clear description

## License

This project is licensed under the MIT License.
