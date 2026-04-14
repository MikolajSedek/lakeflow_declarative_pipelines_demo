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
  - [Prerequisites](#prerequisites)
  - [Install the Databricks CLI](#install-the-databricks-cli)
  - [Authenticate with Databricks Free Edition](#authenticate-with-databricks-free-edition)
  - [Deploy with Databricks Asset Bundles from a local IDE](#deploy-with-databricks-asset-bundles-from-a-local-ide)
  - [Automated deployment via GitHub Actions](#automated-deployment-via-github-actions)
  - [Manual notebook-based deployment (no CLI)](#manual-notebook-based-deployment-no-cli)
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
├── src/
│   └── python/
│       └── notebooks/
│           ├── 01.create_fake_data.py          # Databricks notebook – generates fake CSV data
│           ├── 02.simple_pipeline.py           # Databricks notebook – simple materialized views
│           ├── 03.advanced_pipeline.py         # Databricks notebook – full medallion pipeline
│           ├── 04.scd_genie_code_pipeline.py   # Databricks notebook – SCD Type 2 historical tracking
│           └── modules/
│               ├── __init__.py
│               ├── data_generation.py          # Pure-Python data generation helpers (no Spark dependency)
│               └── transformations.py          # Pure PySpark transformation functions
├── tests/
│   ├── conftest.py                    # Shared fixtures (SparkSession, sample DataFrames)
│   ├── test_data_generation.py        # Tests for data generation (pure Python, no Spark)
│   ├── test_parallel_writes.py        # Tests for concurrent DataFrame writes (Spark)
│   ├── test_pipelines.py              # Tests for pipeline decorator registration (no Spark)
│   ├── test_simple_pipeline.py        # Tests for simple materialized view pipeline (no Spark)
│   ├── test_advanced_pipeline.py      # Tests for advanced medallion pipeline registration (no Spark)
│   ├── test_scd_genie_code_pipeline.py # Tests for SCD Type 2 pipeline (no Spark)
│   ├── test_joinability.py            # Tests for join correctness and aggregation (Spark)
│   └── test_transformations.py        # Tests for transformation functions (Spark)
├── databricks.yml              # Databricks Asset Bundle – workflows, pipelines, targets
├── pyproject.toml              # Project metadata, tool configuration
├── requirements.txt            # Runtime + test dependencies
├── apm.yml                     # Agent Package Manager skill declarations
├── .pre-commit-config.yaml     # Pre-commit hook definitions
└── .github/workflows/
    └── ci.yml                  # GitHub Actions CI/CD – lint + unit tests on feature branches / PRs; deploy job handles bundle deployment on push to main / test
```

---

## Pipeline Notebooks

### 01 – Fake Data Generation

**File:** `src/python/notebooks/01.create_fake_data.py`

Generates synthetic datasets using the [mimesis](https://mimesis.name/) library and writes them as CSV files to Unity Catalog Volumes.

| Dataset        | Key Columns                                             | Volume   |
|----------------|---------------------------------------------------------|----------|
| `fake_users`   | id, Person\_Name, person\_surname, personal\_email, … | CSV      |
| `fake_products`| id, product\_name, price, stock, company\_name, …     | CSV      |
| `fake_orders`  | id, userid, productid, price, product\_name, …        | CSV      |

**Key design decisions:**
- Column naming is **intentionally inconsistent** across all three datasets (e.g. `Person_Name` vs `person_surname`, `productid` vs `product_name`) to simulate real-world messy sources and demonstrate downstream normalization in the Silver layer.
- **Joinable foreign keys:** Orders reference valid `userid` (→ `fake_users.id`) and `productid` (→ `fake_products.id`) values, enabling realistic multi-table joins in the Gold layer.
- **Varied timestamps:** Order timestamps are spread over a 30-day window so that SCD Type 2 tracking and temporal analysis produce meaningful results.
- Each dataset includes a `nonsense_column` to demonstrate column pruning.
- Writes use `ThreadPoolExecutor` for concurrent I/O – Spark write actions release the GIL, so threads provide a real speedup over sequential writes.

### 02 – Simple Pipeline

**File:** `src/python/notebooks/02.simple_pipeline.py`

A minimal Lakeflow Declarative Pipeline that reads CSV sources and creates **materialized views** using `@dp.materialized_view`.  This notebook demonstrates:

- Dynamic table creation via a factory function (`create_simple_materialized_view`)
- Parameterized source paths and target catalog/schema
- The `pyspark.pipelines` declarative API

### 03 – Advanced Pipeline (Medallion)

**File:** `src/python/notebooks/03.advanced_pipeline.py`

The full **Bronze → Silver → Gold** medallion pipeline with:

| Stage  | Function                            | What It Does                                                              |
|--------|-------------------------------------|---------------------------------------------------------------------------|
| Bronze | `create_raw_bronze_table`           | Reads streaming CSV via Auto Loader (`cloudFiles`), adds file metadata and load timestamp |
| Silver | `create_silver_staging_table`       | Drops nonsense columns, lowercases all column names, SHA-2 hashes sensitive data |
| Gold   | `create_gold_merged_table`          | Creates a streaming table and applies CDC merge via `dp.create_auto_cdc_flow` |
| Agg    | `aggregate_gold_tables`             | Unions row/distinct-ID counts across all gold tables into a summary materialized view |
| KPI    | `create_gold_revenue_per_product`   | Joins orders with products to compute revenue, order count, and avg order value per product |
| KPI    | `create_gold_customer_order_summary`| Joins orders with users to compute total spend, order count, and avg order value per customer |
| KPI    | `create_gold_orders_enriched`       | Three-way join (orders → users → products) producing a wide fact table for BI dashboards |
| KPI    | `create_gold_revenue_by_geography`  | Joins orders with users to compute revenue, order count, unique customers, and avg order value per country/city |
| KPI    | `create_gold_company_sales_performance` | Joins orders with products to compute revenue, order count, unique products sold, and avg order value per company/brand |
| KPI    | `create_gold_top_products_by_country` | Three-way join (orders → users → products) to compute revenue and order count per product per country |

**Configuration** is centralized in the `TablePipelineConfig` frozen dataclass (Pydantic), making it easy to override catalogs, schemas, and key columns per environment.

### 04 – SCD Type 2 Pipeline

**File:** `src/python/notebooks/04.scd_genie_code_pipeline.py`

A specialized pipeline implementing **Slowly Changing Dimension Type 2** (SCD Type 2) for tracking historical changes in business entities:

| Feature                  | Configuration                                                 |
|--------------------------|---------------------------------------------------------------|
| **Source**               | `test_catalog.test_silver_schema.fake_orders_staging`         |
| **Target**               | `test_catalog.test_gold_schema.fake_orders_scd2`              |
| **Primary Key**          | `id`                                                          |
| **Sequence Column**      | `timestamp`                                                   |
| **History Tracking**     | `productid`, `userid` (other columns use SCD Type 1 semantics)|
| **Output Columns**       | Original columns + `__START_AT` + `__END_AT` (temporal validity) |

**Key capabilities:**
- **Selective history tracking:** Tracks changes to `productid` and `userid` columns using `track_history_column_list`. Changes to other columns (like `price` or `product_name`) update the current row without creating history.
- **Joinable foreign keys:** With proper `userid` → users and `productid` → products references, SCD2 history records can be joined to dimension tables at any point in time.
- **Temporal validity:** The target table automatically includes `__START_AT` and `__END_AT` columns that mark when each version of a record was valid.
- **Point-in-time queries:** Query historical state at any point in time or join facts to the dimension version that was active during a transaction.
- **Audit trail:** Maintains complete history of product and customer changes for compliance and analysis.

**Use cases:**
- Track product reassignments in order history
- Track customer reassignments (account merges, fraud re-attribution)
- Maintain customer address history for compliance
- Audit dimension changes over time
- Enable temporal joins between facts and dimensions

---

## Library Modules

### modules/data\_generation.py

Pure-Python module with **no SparkSession dependency**.  Contains:

- **`generate_list_of_rows(row_type, num_rows, locale, *, num_users, num_products)`** – Generates a list of PySpark `Row` objects for `"users"`, `"products"`, or `"orders"`.  Includes input validation for both `row_type` and `num_rows`.  For orders, optional `num_users` and `num_products` keyword arguments constrain foreign key columns (`userid`, `productid`) to valid ID ranges, ensuring joins produce non-empty results.
- **`FrameConfig`** – A `NamedTuple` pairing a table name with its DataFrame.

Design notes:
- Threading/multiprocessing was benchmarked and found slower than sequential generation for the current scale (≤325K rows) due to GIL contention and serialization overhead.
- Uses `mimesis` providers for realistic fake data and `pendulum` for ISO 8601 timestamps.

### modules/transformations.py

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
| `test_data_generation`         | 21    | Row counts, field names, value ranges, input validation, FrameConfig, foreign key ranges |
| `test_transformations`         | 24    | Column addition/removal, lowercasing, hashing, edge cases, defaults, validation |
| `test_parallel_writes`         | 13    | Concurrent writes, append semantics, error propagation, edge cases |
| `test_pipelines`               | 31    | Decorator registration, flow creation, name inference, config freezing |
| `test_scd_genie_code_pipeline` | 24    | SCD Type 2 pipeline constants, streaming table registration, CDC flow invocation |
| `test_simple_pipeline`         | 8     | `create_simple_materialized_view` registration, name generation, `create_tables` |
| `test_advanced_pipeline`       | 52    | Bronze/silver/gold table registration, CDC invocation, `TablePipelineConfig`, aggregation, KPI tables (incl. geography, company sales, top products by country) |
| `test_joinability`             | 16    | Join correctness, FK integrity, aggregation verification for all gold KPI patterns |

The test suite uses a **session-scoped** local `SparkSession` (`local[1]`, UI disabled, 1 shuffle partition) to minimize JVM startup overhead.

---

## Code Quality & Linting

The project enforces strict code quality through a comprehensive pre-commit configuration:

| Tool          | Purpose                                    | Scope                        |
|---------------|--------------------------------------------|------------------------------|
| **Black**     | Uncompromising Python code formatting       | All Python files             |
| **Ruff**      | Linting (E/F/W/I/B/S/UP/C4/SIM/T20/RUF/PT/PERF) + formatting | All Python files |
| **mypy**      | Static type checking                       | `src/python/notebooks/modules/transformations.py`, `src/python/notebooks/modules/data_generation.py` |
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

### Continuous Integration

GitHub Actions (`.github/workflows/ci.yml`) runs three jobs on **every push** (all branches) and on every **pull request targeting `main` or `test`**:

| Job          | What It Does                               | Timeout |
|--------------|--------------------------------------------|---------|
| `lint`       | Runs all pre-commit hooks (incl. yamllint) | 10 min  |
| `test-pure`  | Pure-Python tests (`-m "not spark"`)       | 10 min  |
| `test-spark` | PySpark tests (`-m spark`)                 | 15 min  |

All jobs use pip caching for fast dependency installation.

### Continuous Deployment

The **same** `ci.yml` workflow contains a `deploy` job that runs **only after `lint`, `test-pure`, and `test-spark` all pass** and only on direct pushes to `main` or `test`:

| Branch | Target  | What Happens                                                      |
|--------|---------|-------------------------------------------------------------------|
| `test` | `dev`   | Deploys all bundle resources with development mode (name-prefixed) |
| `main` | `prod`  | Deploys all bundle resources in production mode                   |

The deploy job authenticates using a Personal Access Token (PAT).  Configure the following GitHub repository secrets before enabling automated deployments:

| Secret             | Description                                                              |
|--------------------|--------------------------------------------------------------------------|
| `DATABRICKS_HOST`  | Workspace URL, e.g. `https://adb-1234567890123456.7.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Databricks Personal Access Token (PAT)                                   |

See [Automated deployment via GitHub Actions](#automated-deployment-via-github-actions) for step-by-step setup.

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
| [`databricks-docs`](https://github.com/databricks-solutions/ai-dev-kit/tree/main/databricks-skills/databricks-docs) | Query and reason over official Databricks documentation |
| [`databricks-jobs`](https://github.com/databricks-solutions/ai-dev-kit/tree/main/databricks-skills/databricks-jobs) | Create, manage, and debug Databricks Jobs |
| [`databricks-bundles`](https://github.com/databricks-solutions/ai-dev-kit/tree/main/databricks-skills/databricks-bundles) | Scaffold, validate, and deploy Databricks Asset Bundles |
| [`databricks-spark-declarative-pipelines`](https://github.com/databricks-solutions/ai-dev-kit/tree/main/databricks-skills/databricks-spark-declarative-pipelines) | Author and manage Lakeflow Declarative Pipelines |

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

The project ships with a `databricks.yml` file that defines all resources as a **Databricks Asset Bundle (DAB)**.  You can deploy using the Databricks CLI from your local machine or via GitHub Actions.

### Prerequisites

- **Databricks Free Edition** workspace with **Unity Catalog** enabled
- Create the required catalog, schemas, and volume once (SQL console or notebook):

```sql
CREATE CATALOG IF NOT EXISTS test_catalog;
CREATE SCHEMA IF NOT EXISTS test_catalog.test_schema;
CREATE VOLUME IF NOT EXISTS test_catalog.test_schema.test_volume;
CREATE SCHEMA IF NOT EXISTS test_catalog.test_bronze_schema;
CREATE SCHEMA IF NOT EXISTS test_catalog.test_silver_schema;
CREATE SCHEMA IF NOT EXISTS test_catalog.test_gold_schema;
```

---

### Install the Databricks CLI

The Databricks CLI (v0.200+) is required to work with Asset Bundles.

**Linux / macOS (recommended)**

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
databricks --version   # verify installation
```

**macOS via Homebrew**

```bash
brew tap databricks/tap
brew install databricks
databricks --version
```

**Windows (PowerShell)**

```powershell
winget install Databricks.DatabricksCLI
# or download the MSI from https://github.com/databricks/cli/releases
```

For the full installation guide, see the [official docs](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/install).

---

### Authenticate with Databricks Free Edition

The CLI supports several authentication methods.  For the **Databricks Free Edition** the easiest options are:

#### Option A – Interactive OAuth (personal access, recommended for local dev)

```bash
databricks auth login --host https://<your-workspace-url>.azuredatabricks.net
# A browser window opens; log in with your Databricks account.
# Credentials are saved to ~/.databrickscfg under the [DEFAULT] profile.
```

Verify the connection:

```bash
databricks current-user me
```

#### Option B – Personal Access Token (PAT)

1. In the Databricks workspace, go to **Settings → Developer → Access tokens → Generate new token**.
2. Copy the token and run:

```bash
databricks configure --host https://<your-workspace-url>.azuredatabricks.net \
                     --token
# Paste your PAT when prompted.
```

#### Option C – Personal Access Token (PAT, for CI/CD)

This is what the GitHub Actions deploy workflow uses.  Set the following environment variables before running CLI commands:

```bash
export DATABRICKS_HOST=https://<your-workspace-url>.azuredatabricks.net
export DATABRICKS_TOKEN=<your-personal-access-token>
```

To generate a Personal Access Token on **Databricks Free Edition**:
1. In your Databricks workspace, click your username in the top bar and select **Settings**.
2. Click **Developer → Manage** (next to **Access tokens**).
3. Click **Generate new token**, enter a comment and lifetime, then click **Generate**.
4. Copy the token immediately — it is only shown once.

For a full reference, see [Databricks personal access tokens](https://docs.databricks.com/aws/en/dev-tools/auth/pat).

---

### Deploy with Databricks Asset Bundles from a local IDE

The `databricks.yml` at the repository root defines three workflows and two pipelines.

**Bundle resources**

| Resource key | Type | Databricks name | Description |
|---|---|---|---|
| `create_fake_data_workflow` | Job | `01.create_fake_data_workflow` | Runs the fake-data generation notebook on serverless compute |
| `run_simple_lakeflow_pipeline_workflow` | Job | `02.run_simple_lakeflow_pipeline_workflow` | Triggers the simple pipeline |
| `run_advanced_scd_pipeline_workflow` | Job | `03.run_advanced_scd_pipeline_workflow` | Triggers the advanced + SCD pipeline |
| `simple_pipeline` | Pipeline | `02.simple_lakeflow_pipeline` | Materialized views from CSV sources |
| `advanced_scd_pipeline` | Pipeline | `03.advanced_scd_lakeflow_pipeline` | Full medallion + SCD Type 2 pipeline |

**Step-by-step local deployment**

```bash
# 1. Clone the repository
git clone https://github.com/MikolajSedek/lakeflow_declarative_pipelines_demo.git
cd lakeflow_declarative_pipelines_demo

# 2. Authenticate (once; see section above)
databricks auth login --host https://<workspace-url>.azuredatabricks.net

# 3. Validate the bundle (dry-run, no changes deployed)
databricks bundle validate

# 4. Deploy to the 'dev' target (default)
databricks bundle deploy

# 5. (Optional) Run a specific workflow or pipeline
databricks bundle run create_fake_data_workflow
databricks bundle run simple_pipeline
databricks bundle run run_simple_lakeflow_pipeline_workflow
databricks bundle run advanced_scd_pipeline
databricks bundle run run_advanced_scd_pipeline_workflow

# 6. (Optional) Tear down all deployed resources
databricks bundle destroy --auto-approve
```

**Deploy to production**

```bash
databricks bundle deploy --target prod --auto-approve
```

---

### Automated deployment via GitHub Actions

`.github/workflows/ci.yml` runs lint and all tests on every push, then automatically deploys the bundle **only after all checks pass**:

- **push to `test`** → deploys to the `dev` target (development mode, resource names are prefixed with `[dev <username>]`)
- **push to `main`** → deploys to the `prod` target

**Setup**

1. Generate a **Personal Access Token** in your workspace (see [Option C](#option-c--personal-access-token-pat-for-cicd) above).
2. Add two secrets to your GitHub repository (**Settings → Secrets and variables → Actions → New repository secret**):

| Secret name        | Value                                                                    |
|--------------------|--------------------------------------------------------------------------|
| `DATABRICKS_HOST`  | Your workspace URL, e.g. `https://adb-1234567890123456.7.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Databricks Personal Access Token (PAT)                                   |

3. Merge a branch into `test` or `main`.  The deploy workflow will run automatically.

---

### Manual notebook-based deployment (no CLI)

If you prefer not to use the CLI:

1. **Import** the repository into your Databricks workspace (Workspace → Git folders → Add Git folder).
2. **Run `src/python/notebooks/01.create_fake_data.py`** as a notebook to generate CSV source data in the volume.
3. **Create an ETL Pipeline** (Lakeflow Declarative Pipeline) in the Databricks UI:
   - Set the default catalog to `test_catalog`.
   - Add `src/python/notebooks/03.advanced_pipeline.py` (or `02.simple_pipeline.py` / `04.scd_genie_code_pipeline.py`) as the pipeline source.
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
- [Databricks CLI – Install & overview](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/) – Official CLI documentation
- [Databricks CLI – Authentication](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/authentication) – All supported auth methods
- [Databricks Asset Bundles overview](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/) – DAB reference docs
- [Bundle deployment modes](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/deployment-modes) – dev vs prod targets
- [Pipelines CLI reference](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/reference/pipelines-commands) – `databricks pipelines` commands
