# Lakeflow Declarative Pipelines Demo

End-to-end demonstration of **Databricks Lakeflow Declarative Pipelines** (the successor to Delta Live Tables) running on the **Databricks Free Edition**.  The project implements the **Medallion Architecture** (Bronze → Silver → Gold) using `pyspark.pipelines` (PySpark ≥ 4.0) and includes a full local test suite that runs without a Databricks cluster.

> **Companion article:** [Deploy streaming, incremental and batch processing ETLs using Lakeflow Declarative Pipelines](https://medium.com/@mikoajsdek/deploy-streaming-incremental-and-batch-processing-etls-using-lakeflow-declarative-pipelines-in-ba2bc303ed4b)

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Repository Structure](#repository-structure)
- [Pipeline Notebooks](#pipeline-notebooks)
  - [01 – Fake Data Generation](#01--fake-data-generation)
  - [02 – Simple Pipeline](#02--simple-pipeline)
  - [03 – Advanced Pipeline (Medallion)](#03--advanced-pipeline-medallion)
  - [04 – SCD Type 2 Pipeline](#04--scd-type-2-pipeline)
- [Library Modules](#library-modules)
  - [data\_generation.py](#data_generationpy)
  - [transformations.py](#transformationspy)
- [Testing](#testing)
  - [Test Categories](#test-categories)
  - [Running Tests Locally](#running-tests-locally)
- [Code Quality & Linting](#code-quality--linting)
- [CI / CD](#ci--cd)
- [Agent Package Manager (APM)](#agent-package-manager-apm)
- [Databricks Deployment](#databricks-deployment)
- [Key API Reference](#key-api-reference)
- [References & Further Reading](#references--further-reading)

---

## Architecture Overview

The project follows the **Medallion Architecture** pattern widely used in Databricks lakehouse environments:

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   BRONZE (Raw)  │────▶│  SILVER (Staging) │────▶│  GOLD (Clean)   │
│                 │     │                   │     │                 │
│ • Auto Loader   │     │ • Drop columns    │     │ • CDC merge     │
│ • File metadata │     │ • Lowercase cols  │     │ • Deduplication │
│ • Load timestamp│     │ • SHA-2 hashing   │     │ • Aggregations  │
└─────────────────┘     └──────────────────┘     └─────────────────┘
         ▲                       │                        │
         │                       ▼                        ▼
   CSV files in          ┌─────────────────┐     ┌─────────────────┐
   Unity Catalog         │  SCD Type 2     │     │  Summary Stats  │
   Volumes               │ (Gold - History)│     │ (Materialized   │
                         │ • Track changes │     │      View)      │
                         │ • __START_AT    │     └─────────────────┘
                         │ • __END_AT      │
                         └─────────────────┘
```

| Layer   | Table Type        | API Used                              | Purpose                                        |
|---------|-------------------|---------------------------------------|------------------------------------------------|
| Bronze  | Streaming Table   | `@dp.table`                           | Raw ingestion with metadata & timestamps        |
| Silver  | Streaming Table   | `@dp.table`                           | Cleansing, anonymization, column normalization   |
| Gold    | Streaming Table   | `dp.create_streaming_table` + CDC     | Deduplicated, merge-ready business entities      |
| Summary | Materialized View | `@dp.materialized_view`               | Cross-table aggregation statistics               |
| SCD2    | Streaming Table   | `dp.create_streaming_table` + CDC Type 2 | Historical tracking with temporal validity; reads from Silver, writes to Gold schema |

---

## Repository Structure

```
.
├── 01.create_fake_data.py      # Databricks notebook – generates fake CSV data
├── 02.simple_pipeline.py       # Databricks notebook – simple materialized views
├── 03.advanced_pipeline.py     # Databricks notebook – full medallion pipeline
├── 04.scd_genie_code_pipeline.py # Databricks notebook – SCD Type 2 historical tracking
├── data_generation.py          # Pure-Python data generation helpers (no Spark dependency)
├── transformations.py          # Pure PySpark transformation functions
├── tests/
│   ├── conftest.py                    # Shared fixtures (SparkSession, sample DataFrames)
│   ├── test_data_generation.py        # Tests for data generation (pure Python, no Spark)
│   ├── test_parallel_writes.py        # Tests for concurrent DataFrame writes (Spark)
│   ├── test_pipelines.py              # Tests for pipeline decorator registration (no Spark)
│   ├── test_simple_pipeline.py        # Tests for simple materialized view pipeline (no Spark)
│   ├── test_advanced_pipeline.py      # Tests for advanced medallion pipeline registration (no Spark)
│   └── test_transformations.py        # Tests for transformation functions (Spark)
├── pyproject.toml              # Project metadata, tool configuration
├── requirements.txt            # Runtime + test dependencies
├── .pre-commit-config.yaml     # Pre-commit hook definitions
└── .github/workflows/ci.yml   # GitHub Actions CI pipeline
```

---

## Pipeline Notebooks

### 01 – Fake Data Generation

**File:** `01.create_fake_data.py`

Generates synthetic datasets using the [mimesis](https://mimesis.name/) library and writes them as CSV files to Unity Catalog Volumes.

| Dataset        | Key Columns                                             | Volume   |
|----------------|---------------------------------------------------------|----------|
| `fake_users`   | id, Person\_Name, person\_surname, personal\_email, … | CSV      |
| `fake_products`| id, product\_name, price, stock, company\_name, …     | CSV      |
| `fake_orders`  | id, productid, price, product\_name, …                | CSV      |

**Key design decisions:**
- Column naming is **intentionally inconsistent** across all three datasets (e.g. `Person_Name` vs `person_surname`, `productid` vs `product_name`) to simulate real-world messy sources and demonstrate downstream normalization in the Silver layer.
- Each dataset includes a `nonsense_column` to demonstrate column pruning.
- Writes use `ThreadPoolExecutor` for concurrent I/O – Spark write actions release the GIL, so threads provide a real speedup over sequential writes.

### 02 – Simple Pipeline

**File:** `02.simple_pipeline.py`

A minimal Lakeflow Declarative Pipeline that reads CSV sources and creates **materialized views** using `@dp.materialized_view`.  This notebook demonstrates:

- Dynamic table creation via a factory function (`create_simple_materialized_view`)
- Parameterized source paths and target catalog/schema
- The `pyspark.pipelines` declarative API

### 03 – Advanced Pipeline (Medallion)

**File:** `03.advanced_pipeline.py`

The full **Bronze → Silver → Gold** medallion pipeline with:

| Stage  | Function                       | What It Does                                                              |
|--------|--------------------------------|---------------------------------------------------------------------------|
| Bronze | `create_raw_bronze_table`      | Reads streaming CSV via Auto Loader (`cloudFiles`), adds file metadata and load timestamp |
| Silver | `create_silver_staging_table`  | Drops nonsense columns, lowercases all column names, SHA-2 hashes sensitive data |
| Gold   | `create_gold_merged_table`     | Creates a streaming table and applies CDC merge via `dp.create_auto_cdc_flow` |
| Agg    | `aggregate_gold_tables`        | Unions row/distinct-ID counts across all gold tables into a summary materialized view |

**Configuration** is centralized in the `TablePipelineConfig` frozen dataclass (Pydantic), making it easy to override catalogs, schemas, and key columns per environment.

### 04 – SCD Type 2 Pipeline

**File:** `04.scd_genie_code_pipeline.py`

A specialized pipeline implementing **Slowly Changing Dimension Type 2** (SCD Type 2) for tracking historical changes in business entities:

| Feature                  | Configuration                                                 |
|--------------------------|---------------------------------------------------------------|
| **Source**               | `test_catalog.test_silver_schema.fake_orders_staging`         |
| **Target**               | `test_catalog.test_gold_schema.fake_orders_scd2`              |
| **Primary Key**          | `id`                                                          |
| **Sequence Column**      | `timestamp`                                                   |
| **History Tracking**     | `productid` only (other columns use SCD Type 1 semantics)     |
| **Output Columns**       | Original columns + `__START_AT` + `__END_AT` (temporal validity) |

**Key capabilities:**
- **Selective history tracking:** Only tracks changes to the `productid` column using `track_history_column_list`. Changes to other columns (like `price` or `product_name`) update the current row without creating history.
- **Temporal validity:** The target table automatically includes `__START_AT` and `__END_AT` columns that mark when each version of a record was valid.
- **Point-in-time queries:** Query historical state at any point in time or join facts to the dimension version that was active during a transaction.
- **Audit trail:** Maintains complete history of product changes for compliance and analysis.

**Use cases:**
- Track product reassignments in order history
- Maintain customer address history for compliance
- Audit dimension changes over time
- Enable temporal joins between facts and dimensions

---

## Library Modules

### data\_generation.py

Pure-Python module with **no SparkSession dependency**.  Contains:

- **`generate_list_of_rows(row_type, num_rows, locale)`** – Generates a list of PySpark `Row` objects for `"users"`, `"products"`, or `"orders"`.  Includes input validation for both `row_type` and `num_rows`.
- **`FrameConfig`** – A `NamedTuple` pairing a table name with its DataFrame.

Design notes:
- Threading/multiprocessing was benchmarked and found slower than sequential generation for the current scale (≤325K rows) due to GIL contention and serialization overhead.
- Uses `mimesis` providers for realistic fake data and `pendulum` for ISO 8601 timestamps.

### transformations.py

Pure PySpark transformation functions with **no framework dependencies** (no DLT/LDP globals):

| Function                          | Description                                                     |
|-----------------------------------|-----------------------------------------------------------------|
| `add_load_timestamp`              | Appends a `current_timestamp()` column                          |
| `extract_file_name_from_metadata` | Extracts `file_name` from the `_metadata` struct (type-safe, no SQL injection) |
| `lower_all_column_names`          | Lowercases every column name                                    |
| `remove_nonsense_columns`         | Drops specified columns, silently ignoring absent ones           |
| `anonymize_sensitive_data`        | Replaces sensitive column values with SHA-2 hashes (validated bit lengths: 0, 224, 256, 384, 512) |

All functions follow the **single-DataFrame-in, single-DataFrame-out** pattern, making them composable with `.transform()`.

---

## Testing

### Test Categories

Tests are split into two categories using `pytest` markers:

| Marker      | Description                              | Requires Spark? |
|-------------|------------------------------------------|-----------------|
| *(none)*    | Pure Python tests (data generation)      | No              |
| `spark`     | PySpark tests (transformations, parallel writes)   | Yes (local)     |

### Running Tests Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run ALL tests
pytest tests/ -v

# Run only pure-Python tests (fast, no JVM)
pytest tests/ -m "not spark" -v

# Run only PySpark tests
pytest tests/ -m spark -v
```

### Test Coverage Summary

| Test Module                    | Tests | What's Covered                                                  |
|--------------------------------|-------|-----------------------------------------------------------------|
| `test_data_generation`         | 16    | Row counts, field names, value ranges, input validation, FrameConfig |
| `test_transformations`         | 24    | Column addition/removal, lowercasing, hashing, edge cases, defaults, validation |
| `test_parallel_writes`         | 13    | Concurrent writes, append semantics, error propagation, edge cases |
| `test_pipelines`               | 31    | Decorator registration, flow creation, name inference, config freezing |
| `test_scd_genie_code_pipeline` | 24    | SCD Type 2 pipeline constants, streaming table registration, CDC flow invocation |
| `test_simple_pipeline`         | 8     | `create_simple_materialized_view` registration, name generation, `create_tables` |
| `test_advanced_pipeline`       | 22    | Bronze/silver/gold table registration, CDC invocation, `TablePipelineConfig`, aggregation |

The test suite uses a **session-scoped** local `SparkSession` (`local[1]`, UI disabled, 1 shuffle partition) to minimize JVM startup overhead.

---

## Code Quality & Linting

The project enforces strict code quality through a comprehensive pre-commit configuration:

| Tool          | Purpose                                    | Scope                        |
|---------------|--------------------------------------------|------------------------------|
| **Ruff**      | Linting (E/F/W/I/B/S/UP/C4/SIM/T20/RUF/PT/PERF) + formatting | All Python files |
| **mypy**      | Static type checking                       | `transformations.py`, `data_generation.py` |
| **Bandit**    | Security linting                           | Production modules           |
| **pydocstyle**| Google-style docstring enforcement         | Production modules           |
| **interrogate**| Docstring coverage (≥95%)                 | All modules                  |
| **codespell** | Typo detection                             | All files                    |
| **pyupgrade** | Auto-upgrade to Python 3.10+ syntax        | All Python files             |

```bash
# Run all pre-commit checks
pre-commit run --all-files
```

---

## CI / CD

GitHub Actions (`.github/workflows/ci.yml`) runs three jobs on every **push to feature branches** and on every **pull request targeting `main` or `test`**:

| Job          | What It Does                               | Timeout |
|--------------|--------------------------------------------|---------|
| `lint`       | Runs all pre-commit hooks                  | 10 min  |
| `test-pure`  | Pure-Python tests (`-m "not spark"`)       | 10 min  |
| `test-spark` | PySpark tests (`-m spark`)                 | 15 min  |

All jobs use pip caching for fast dependency installation.

---

## Agent Package Manager (APM)

This project uses [APM](https://github.com/microsoft/apm) — an open-source dependency manager for AI agents — to declare and install agent skills, prompts, and plugins reproducibly alongside the code.

The `apm.yml` manifest at the root of the repository declares all agentic dependencies:

```yaml
name: lakeflow-declarative-pipelines-demo
version: 0.1.0
dependencies:
  apm:
    - github/awesome-copilot/skills/pytest-coverage
```

### Installed Skills

| Skill | Description |
|-------|-------------|
| [`pytest-coverage`](https://github.com/github/awesome-copilot/blob/main/skills/pytest-coverage/SKILL.md) | Run pytest with coverage, identify uncovered lines, and iteratively improve coverage to 100% |
| [`pyspark-style-guide`](.github/copilot/skills/pyspark-style-guide/SKILL.md) | Write idiomatic, performant PySpark code following the [Palantir PySpark Style Guide](https://github.com/palantir/pyspark-style-guide) |

### Setup

Install APM and configure your AI agent with the project's agentic dependencies:

```bash
# Install APM (Linux / macOS)
curl -sSL https://aka.ms/apm-unix | sh

# Install all declared agent dependencies
apm install
```

After running `apm install`, your AI coding agent (GitHub Copilot, Claude Code, Cursor, etc.) will automatically have the `pytest-coverage` skill available, enabling it to run coverage analysis and improve test coverage.

---

## Databricks Deployment

### Prerequisites

- **Databricks Free Edition** workspace with **Unity Catalog** enabled
- A catalog, schemas, and volume for storing source data:

```sql
CREATE CATALOG IF NOT EXISTS test_catalog;
CREATE SCHEMA IF NOT EXISTS test_catalog.test_schema;
CREATE VOLUME IF NOT EXISTS test_catalog.test_schema.test_volume;
CREATE SCHEMA IF NOT EXISTS test_catalog.test_bronze_schema;
CREATE SCHEMA IF NOT EXISTS test_catalog.test_silver_schema;
CREATE SCHEMA IF NOT EXISTS test_catalog.test_gold_schema;
```

### Steps

1. **Import** the repository into your Databricks workspace.
2. **Run `01.create_fake_data.py`** as a notebook to generate CSV source data in the volume.
3. **Create an ETL Pipeline** (Lakeflow Declarative Pipeline) in the Databricks UI:
   - Set the default catalog to `test_catalog`.
   - Add `03.advanced_pipeline.py` (or `02.simple_pipeline.py` or `04.scd_genie_code_pipeline.py`) as the pipeline source.
4. **Start** the pipeline – Databricks handles orchestration, dependency resolution, and incremental processing automatically.

> **Note:** `dp.create_auto_cdc_flow` is a Databricks-only API and is not available in the open-source `pyspark.pipelines` module.

---

## Key API Reference

| API                            | Type              | Description                                                          |
|--------------------------------|-------------------|----------------------------------------------------------------------|
| `@dp.table`                    | Decorator         | Declares a streaming table with an associated data flow              |
| `@dp.materialized_view`       | Decorator         | Declares a materialized view (batch, precomputed)                    |
| `@dp.temporary_view`          | Decorator         | Declares a session-scoped temporary view                             |
| `dp.create_streaming_table`   | Imperative API    | Creates a streaming table without an immediate flow (for CDC targets)|
| `dp.create_auto_cdc_flow`     | Imperative API    | Applies CDC (SCD Type 1/2) merge from a source to a target table     |
| `@dp.append_flow`             | Decorator         | Adds an additional data flow to an existing streaming table          |

For full API documentation, see the [Spark Declarative Pipelines Programming Guide](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html) and the [Databricks Python Reference](https://docs.databricks.com/gcp/en/ldp/developer/python-dev).

---

## References & Further Reading

- [Spark Declarative Pipelines Programming Guide](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html) – Open-source API docs
- [Databricks Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/) – Official Databricks documentation
- [Bringing Declarative Pipelines to Apache Spark](https://www.databricks.com/blog/bringing-declarative-pipelines-apache-spark-open-source-project) – Databricks engineering blog
- [Medallion Architecture Explained](https://pipelinepulse.dev/medallion-architecture-explained/) – Bronze/Silver/Gold pattern guide
- [CDC with create\_auto\_cdc\_flow](https://docs.databricks.com/aws/en/ldp/cdc) – Change Data Capture API reference
- [Unity Catalog with Pipelines](https://learn.microsoft.com/en-us/azure/databricks/ldp/unity-catalog) – Governance & permissions
