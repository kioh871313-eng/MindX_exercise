# Airflow ETL Pipeline — Computer Hardware Sales

An end-to-end data pipeline built with **Apache Airflow**, **PostgreSQL**, and **Docker** that extracts Computer Hardware Sales data from **Amazon S3**, transforms it through a **Medallion architecture** (Bronze → Silver → Gold), and loads it into a star-schema data mart for reporting.

> **Design principle:** All data cleaning, transformation, and modelling logic lives in `.sql` files under `sqls/`. Python (DAG code) is responsible only for orchestration, parameter passing, and writing execution logs. This keeps transformation logic reviewable, testable, and database-native.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Project Structure](#project-structure)
3. [Tech Stack](#tech-stack)
4. [Quick Start](#quick-start)
5. [Airflow Connections & Variables](#airflow-connections--variables)
6. [ETL Pipeline Details](#etl-pipeline-details)
7. [SQL Transformation Files](#sql-transformation-files)
8. [Data Quality & Validation](#data-quality--validation)
9. [SCD2 Strategy](#scd2-strategy)
10. [Logging & Audit](#logging--audit)
11. [Idempotency & CDC](#idempotency--cdc)
12. [Security Notes](#security-notes)
13. [Generating Sample Source Data](#generating-sample-source-data)
14. [Data Dictionary](#data-dictionary)

---

## Architecture Overview

```
Amazon S3 (CSV files)
        │
        ▼
┌─────────────────────────────────────────────────┐
│                  BRONZE LAYER                   │
│  Raw, unmodified data. All columns TEXT.        │
│  Snapshot model: each load appends a new batch. │
│  Old snapshots auto-pruned after each load.     │
│  schema: bronze  table: raw_hardware_sales      │
└─────────────────────────────────────────────────┘
        │  SQL: transform_bronze_to_silver.sql
        ▼
┌─────────────────────────────────────────────────┐
│                  SILVER LAYER                   │
│  Cleaned, typed, deduplicated.                  │
│  Single source of truth for downstream.         │
│  schema: silver  table: clean_hardware_sales    │
└─────────────────────────────────────────────────┘
        │  SQL: transform_silver_to_dim_*.sql
        │       transform_silver_to_fact_sales.sql
        ▼
┌─────────────────────────────────────────────────┐
│                   GOLD LAYER                    │
│  Star schema data mart for reporting.           │
│  Dims: dim_product (SCD2), dim_customer (SCD2)  │
│        dim_date (Type 1), dim_geography (Type 1)│
│  Fact: fact_sales                               │
│  schema: gold                                   │
└─────────────────────────────────────────────────┘
        │
        ▼
   Reporting / BI Tools
```

All execution events are written to `logs.pipeline_log` for a full audit trail.

---

## Project Structure

```
Project_pipeline_airflow/
├── dags/
│   ├── dag_pipeline.py                 # Single end-to-end pipeline DAG (@hourly)
│   └── scripts/
│       ├── __init__.py
│       ├── s3_helpers.py               # S3Hook wrappers: list files, read CSV, timestamp CDC
│       ├── db_helpers.py               # PostgresHook wrappers: execute_sql_file, write_log
│       └── transformations.py          # SQL runner: one function per .sql transform file
├── sqls/
│   ├── 00_init_schemas.sql             # Auto-run on DWH container first start (DDL only)
│   ├── silver/
│   │   ├── create_silver_schema.sql    # Reference DDL
│   │   └── transform_bronze_to_silver.sql  ← TRANSFORMATION LOGIC
│   ├── bronze/
│   │   └── create_bronze_schema.sql    # Reference DDL
│   ├── gold/
│   │   ├── create_gold_schema.sql      # Reference DDL
│   │   ├── transform_silver_to_dim_date.sql      ← TRANSFORMATION LOGIC
│   │   ├── transform_silver_to_dim_geography.sql ← TRANSFORMATION LOGIC
│   │   ├── transform_silver_to_dim_product.sql   ← TRANSFORMATION LOGIC (SCD2)
│   │   ├── transform_silver_to_dim_customer.sql  ← TRANSFORMATION LOGIC (SCD2)
│   │   ├── transform_silver_to_fact_sales.sql    ← TRANSFORMATION LOGIC (SK resolution)
│   │   └── sample_queries.sql          # Reference analytical queries
│   └── logs_schema/
│       └── create_logs_schema.sql      # Reference DDL
├── docs/
│   └── data_dictionary.md              # Full column-level documentation
├── logs/                               # Airflow task logs (Docker volume)
├── plugins/                            # Custom Airflow plugins (future use)
├── docker-compose.yaml                 # Airflow only — no credentials, safe to commit
├── docker-compose.dev-db.yaml          # git-ignored — local DWH container (dev only)
├── .env.example                        # Template — copy to .env (AIRFLOW_UID only)
├── .gitignore
├── generate_source_data.py             # Developer utility: generate messy sample CSVs
└── README.md
```

---

## Tech Stack

| Component      | Version / Detail                       |
|----------------|----------------------------------------|
| Apache Airflow | 2.8.1                                  |
| PostgreSQL     | 15                                     |
| Docker Compose | v2                                     |
| Python         | 3.11 (bundled in Airflow image)        |
| pandas         | Used only in `generate_source_data.py` and bronze CSV ingest |
| AWS SDK        | `apache-airflow-providers-amazon`      |

---

## Quick Start

### 1. Prerequisites

- Docker Desktop installed and running
- A PostgreSQL database already running and accessible (local or remote)
- AWS credentials with `s3:GetObject` / `s3:ListBucket` on your bucket
- Your CSV file already uploaded to S3

### 2. Configure `.env`

```bash
cp .env.example .env
```

`.env` contains only `AIRFLOW_UID` — no passwords, no keys.

```bash
# On Linux/Mac: replace 50000 with output of: echo $(id -u)
# On Windows with Docker Desktop: leave as 50000
AIRFLOW_UID=50000
```

### 3. (Local dev only) Start the DWH database

If you are running locally and don't have an existing PostgreSQL instance, use the
provided dev database file. **This file is git-ignored** — it exists only on your machine.

Open `docker-compose.dev-db.yaml` and fill in your chosen credentials:

```yaml
POSTGRES_USER:     your_dwh_username   # pick any username
POSTGRES_PASSWORD: your_dwh_password   # pick a strong password
```

Then start it:

```bash
docker compose -f docker-compose.dev-db.yaml up -d
```

This creates the `hardware_sales_dwh` database and auto-runs `sqls/00_init_schemas.sql`
to create all schemas and tables. You only need this step once per machine — the data
persists in a Docker volume across restarts.

> **UAT / PROD:** Skip this step entirely. The database already exists. You just need
> the host, port, database name, username, and password to enter in Airflow UI (step 5).

### 4. Start Airflow

```bash
docker compose up -d
```

Airflow webserver starts at **http://localhost:8080**
Default login: `admin` / `admin`

### 5. Add Connections in Airflow UI

Go to **Admin → Connections → "+" (Add a new record)** and create two connections.
See the [detailed connection guide below](#airflow-connections--variables) for exact field values.

- `aws_default` — AWS S3 credentials
- `postgres_dwh` — Data Warehouse connection

### 6. Add Variables in Airflow UI

Go to **Admin → Variables → "+" (Add)** and add all 4:

| Key | Value | Note |
|-----|-------|------|
| `s3_bucket_name` | `your-bucket-name` | Bucket name only, no `s3://` |
| `s3_prefix` | `computer_hardware_sales/` | Folder path, with trailing slash |
| `pipeline_last_run_ts` | `2000-01-01T00:00:00+00:00` | CDC timestamp checkpoint — set far in the past for initial full load |

### 7. Enable and trigger the DAG

Enable `hardware_sales_pipeline` in the DAG list.
The DAG runs **every hour** automatically. For the first run, trigger it manually
by clicking the ▶ button on the DAG.

---

## Airflow Connections & Variables

> **Security principle:** No credentials exist anywhere in the project folder — not in
> code, SQL files, Docker Compose, or any config file. All passwords and API keys are
> entered once via the Airflow UI and stored encrypted in Airflow's metadata database.

### Where credentials live

| Credential | Where it's stored | In project folder? |
|---|---|---|
| AWS access key | Airflow UI → Connections | No |
| AWS secret key | Airflow UI → Connections | No |
| DWH username | Airflow UI → Connections | No |
| DWH password | Airflow UI → Connections | No |
| Airflow UID | `.env` (git-ignored) | No — not sensitive |

---

### Step-by-step: Create the `aws_default` Connection

1. Open **http://localhost:8080** and log in
2. Click **Admin** in the top menu → **Connections**
3. Click the **"+"** button (Add a new record)
4. Fill in the fields:

   | Field | Value |
   |-------|-------|
   | **Connection Id** | `aws_default` |
   | **Connection Type** | `Amazon Web Services` |
   | **AWS Access Key ID** | Your AWS access key (e.g. `AKIAIOSFODNN7EXAMPLE`) |
   | **AWS Secret Access Key** | Your AWS secret key |

   > If you don't see `Amazon Web Services` in the dropdown, the provider package is not
   > installed. Run: `docker exec airflow_webserver pip install apache-airflow-providers-amazon`
   > then restart the webserver.

5. Click **Test** — you should see `Connection successfully tested`
6. Click **Save**

---

### Step-by-step: Create the `postgres_dwh` Connection

1. Click **Admin → Connections → "+"**
2. Fill in the fields:

   | Field | Value |
   |-------|-------|
   | **Connection Id** | `postgres_dwh` |
   | **Connection Type** | `Postgres` |
   | **Host** | See note below |
   | **Database** | `hardware_sales_dwh` |
   | **Login** | Your DWH username |
   | **Password** | Your DWH password |
   | **Port** | `5432` |

   **What to put in Host:**

   | Your setup | Host value |
   |---|---|
   | Local dev using `docker-compose.dev-db.yaml` | `postgres-dwh` |
   | Local Postgres installed directly on machine | `host.docker.internal` |
   | Remote server / RDS / Cloud SQL | The actual hostname or IP |

   > **Why `postgres-dwh` for local Docker dev?** Airflow runs inside a Docker container.
   > `localhost` inside that container refers to the container itself, not your machine.
   > `postgres-dwh` is the service name on the shared Docker network, which Docker
   > resolves to the correct container automatically.

3. Click **Test** — you should see `Connection successfully tested`
4. Click **Save**

---

### Step-by-step: Add Variables

Variables hold non-sensitive pipeline config (bucket name, CDC checkpoints).

1. Click **Admin → Variables → "+"**
2. Add each row:

   | Key | Value | Description |
   |-----|-------|-------------|
   | `s3_bucket_name` | `your-bucket-name` | S3 bucket name, no `s3://` prefix |
   | `s3_prefix` | `computer_hardware_sales/` | Folder path inside bucket, trailing slash required |
   | `pipeline_last_run_ts` | `2000-01-01T00:00:00+00:00` | CDC timestamp checkpoint — keep far in the past for initial full load. The DAG updates this automatically after every successful run. |

---

### Enterprise security recommendations

For production use:

1. **AWS IAM Roles** (EKS/EC2) — attach an IAM role to the compute instance. Airflow picks it up via the instance metadata service. No access key stored anywhere.
2. **AWS Secrets Manager backend** — configure Airflow to resolve connections directly from Secrets Manager via `airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend`. Connections defined in UI are not needed.
3. **HashiCorp Vault backend** — same pattern for multi-cloud or on-premises.
4. **Fernet key** — set a strong `AIRFLOW__CORE__FERNET_KEY` so passwords stored in the metadata DB are encrypted at rest.

---

## ETL Pipeline Details

### DAG: `hardware_sales_pipeline` (@hourly)

A single DAG replaces the previous three separate DAGs. Each layer triggers
immediately after the previous one completes — no fixed time gaps between layers.

```
capture_checkpoint
  → extract_load_bronze → cleanup_bronze_snapshots → validate_bronze
      → transform_silver → validate_silver
          → [dim_date, dim_geography, dim_product, dim_customer]  (parallel)
              → build_fact_sales → validate_gold
                  → update_cdc_checkpoint
```

`max_active_runs=1` prevents overlapping runs. If a run is still executing when
the next hourly trigger fires, Airflow skips that trigger.

| Task | What happens |
|------|--------------|
| `capture_checkpoint` | Records the pipeline **start** timestamp in XCom. Used as the new CDC checkpoint if the full pipeline succeeds. |
| `extract_load_bronze` | **ShortCircuitOperator.** Scheduled runs: checks S3 for files with `LastModified > pipeline_last_run_ts`; if none found, returns `False` → all downstream tasks **skipped**. Manual runs: bypasses CDC timestamp check and loads **all** S3 files as a fresh snapshot. |
| `cleanup_bronze_snapshots` | Deletes all but the most recent `_ingested_at` batch per `_source_file`. Keeps the Bronze table bounded while preserving the latest complete snapshot for Silver reprocessing. |
| `validate_bronze` | Checks table is non-empty, required columns present, NULL rate < 50% on `order_id`. Warns on soft issues, raises on hard blockers only. |
| `transform_silver` | Calls `sqls/silver/transform_bronze_to_silver.sql`. Processes only the latest snapshot per source file (`_ingested_at = MAX per _source_file`). All cleaning logic (date parsing, type casting, normalisation, measure recalculation) is in the SQL file. |
| `validate_silver` | 13 SQL checks. See [Data Quality & Validation](#data-quality--validation). |
| `build_dim_*` | 4 dimension tasks run in parallel. Each calls the corresponding `sqls/gold/transform_silver_to_dim_*.sql`. |
| `build_fact_sales` | Calls `sqls/gold/transform_silver_to_fact_sales.sql` after all dims complete. Resolves all 5 surrogate keys via SQL JOINs. |
| `validate_gold` | 15 SQL checks across dims and fact. |
| `update_cdc_checkpoint` | Persists the start timestamp (from XCom) as `pipeline_last_run_ts`. Only runs if **all** upstream tasks succeeded — guarantees the checkpoint is never advanced on a failed run. |

### Trigger modes

#### Automatic (hourly schedule)

The pipeline uses `ShortCircuitOperator` for the `extract_load_bronze` task:

```
S3 has new/updated files?
    YES → load to bronze, cleanup old snapshots, run full pipeline
    NO  → short-circuit: all downstream tasks SKIPPED, pipeline marked success
```

Skipped tasks are **not failures** — the DAG run is green. This saves resource on
every hourly run where the source file hasn't changed.

#### Manual trigger (Trigger DAG ▶ button)

Just click **Trigger DAG ▶** in the Airflow UI — no JSON config needed.

The pipeline detects that it was triggered manually (`run_type = "manual"`) and
bypasses the CDC timestamp check, loading **all** S3 files as a fresh snapshot.
`cleanup_bronze_snapshots` then removes the previous snapshot so Silver always
reads the freshly loaded data.

**When to use:** after updating any SQL transform file (e.g. fixing a cleaning rule
in `transform_bronze_to_silver.sql`) to reload source data and reprocess through
Silver and Gold with the new logic.

| Run type | Behaviour |
|---|---|
| **Scheduled (hourly)** | Check CDC timestamp → load only new/updated files → cleanup → full pipeline. Short-circuits if no new files. |
| **Manual (Trigger DAG ▶)** | Bypass CDC timestamp → load **all** S3 files → cleanup old snapshots → full pipeline. |

---

## SQL Transformation Files

All transformation logic is in these files. Edit them to change business rules.

| File | Layer | Type | Key logic |
|------|-------|------|-----------|
| `sqls/silver/transform_bronze_to_silver.sql` | Bronze→Silver | Incremental CDC upsert | Multi-format date parsing (`TO_DATE` with regex routing), comma stripping from numbers, negative value filtering, `INITCAP` normalisation, `COALESCE` for NULL defaults, recalculated measures |
| `sqls/gold/transform_silver_to_dim_date.sql` | Silver→Gold | Type 1 upsert | Derives year/quarter/month/week/day attributes from date using `EXTRACT` and `TO_CHAR` |
| `sqls/gold/transform_silver_to_dim_geography.sql` | Silver→Gold | Type 1 upsert | `SELECT DISTINCT` on geography columns |
| `sqls/gold/transform_silver_to_dim_product.sql` | Silver→Gold | SCD2 | `UPDATE` closes changed records, `INSERT` adds new active versions; uses `IS DISTINCT FROM` for change detection |
| `sqls/gold/transform_silver_to_dim_customer.sql` | Silver→Gold | SCD2 | Same SCD2 pattern as dim_product |
| `sqls/gold/transform_silver_to_fact_sales.sql` | Silver→Gold | Upsert | `LEFT JOIN` to all 4 dimensions to resolve surrogate keys; `ON CONFLICT (order_id) DO UPDATE` |

---

## Data Quality & Validation

Validation runs as a dedicated task after each layer, using SQL queries directly
against the target table. Any failed check raises an exception, marking the DAG
run FAILED and preventing the CDC checkpoint from advancing.

### Bronze validation (`validate_bronze` task) — warn-first philosophy

Bronze is the raw layer. The pipeline accepts dirty data but must confirm
something landed and the structure is usable.

| Check | Fail action |
|-------|-------------|
| Table not empty after load | **Raise** |
| All 6 required columns present (`order_id`, `customer_id`, `product_id`, `order_date`, `unit_price`, `quantity`) | **Raise** |
| NULL rate on `order_id` > 50% | **Raise** (file likely malformed) |
| NULL rate on `order_id` 0–50% | Warn + log only |
| Duplicate `order_id` rate > 30% | Warn + log only |
| No rows ingested in the last 2 hours | Warn + log only |

### Silver validation (`validate_silver` task) — strict, all raise

| Check |
|-------|
| Table not empty |
| No NULL `order_id` |
| No NULL `customer_id` |
| No NULL `product_id` |
| No duplicate `order_id` |
| No negative `quantity` |
| No negative `unit_price` |
| No negative `unit_cost` |
| `discount` in range [0, 1] |
| `profit = total_revenue − total_cost` (tolerance ±0.02) |
| `order_date` NULL rate < 5% |
| `ship_date >= order_date` |
| Silver row count >= 60% of valid bronze row count |

### Gold validation (`validate_gold` task) — strict, all raise

| Check |
|-------|
| `dim_date` not empty |
| `dim_geography` not empty |
| `dim_product` has `is_current=TRUE` records |
| `dim_customer` has `is_current=TRUE` records |
| Each `product_id` has exactly one `is_current=TRUE` row |
| Each `customer_id` has exactly one `is_current=TRUE` row |
| No `valid_from > valid_to` in `dim_product` |
| No `valid_from > valid_to` in `dim_customer` |
| `fact_sales` not empty |
| No NULL `order_id` in `fact_sales` |
| All `dim_product_sk` resolved (no NULL FK) |
| All `dim_customer_sk` resolved (no NULL FK) |
| All `order_date_id` resolved (no NULL FK) |
| `profit = total_revenue − total_cost` (tolerance ±0.02) |
| `fact_sales` row count >= `silver` row count |

---

## SCD2 Strategy

SCD2 is applied to `dim_product` and `dim_customer` **at the Gold layer only**.

**Why Gold only?**
- Bronze is an immutable raw store — SCD2 would pollute it.
- Silver stores one clean row per `order_id` (current state) — it's the input feed, not a history store.
- Gold dimensions are the right place for historical attribute tracking.

**Control columns:**

| Column | Type | Meaning |
|--------|------|---------|
| `valid_from` | DATE | When this version became active |
| `valid_to` | DATE | When superseded (`NULL` = currently active) |
| `is_current` | BOOLEAN | `TRUE` = latest active version |

**SQL implementation:** `transform_silver_to_dim_product.sql` (and `dim_customer`) runs two statements:
1. `UPDATE` — sets `valid_to = TODAY`, `is_current = FALSE` for rows where any tracked attribute changed.
2. `INSERT` — adds a new active row for products/customers with no current active record.

---

## Logging & Audit

Every DAG task writes to `logs.pipeline_log`:

```sql
-- Monitor today's pipeline runs
SELECT dag_id, task_id, layer, status, rows_processed, logged_at
FROM logs.pipeline_log
WHERE logged_at::DATE = CURRENT_DATE
ORDER BY logged_at;

-- Find failures in the last 7 days
SELECT dag_id, task_id, layer, error_message, logged_at
FROM logs.pipeline_log
WHERE status = 'failed'
  AND logged_at >= NOW() - INTERVAL '7 days'
ORDER BY logged_at DESC;
```

---

## Idempotency & CDC

### CDC checkpoint

The pipeline uses a **full UTC timestamp** (not just a date) as its CDC checkpoint,
stored in Airflow Variable `pipeline_last_run_ts`.

**Why timestamp instead of date?**
The same S3 file can be overwritten multiple times per day with new rows appended.
A date-only checkpoint would miss updates from the second run on the same day.
A timestamp checkpoint correctly detects any file whose `LastModified > last run start time`.

**Why save the START time, not the END time?**
If a file is uploaded to S3 while the pipeline is running (between start and end),
its `LastModified` will be between start and end. Saving end time would cause
that file to be missed. Saving start time ensures it is caught on the next run.

**Checkpoint is only advanced on full success.**
The `update_cdc_checkpoint` task is the last task in the DAG. If any upstream
task fails, this task is skipped — the checkpoint stays at its previous value
and the next run re-processes the same files from scratch.

**Short-circuit when no new data.**
When `extract_load_bronze` finds no new S3 files, it returns `False` via
`ShortCircuitOperator`. All downstream tasks are skipped — the checkpoint is
not advanced and no resource is wasted.

**Manual trigger reloads all files.**
Triggering manually bypasses the CDC timestamp check and loads every S3 file
as a fresh snapshot. `cleanup_bronze_snapshots` removes the previous batch
immediately after, so Silver always reads the latest complete snapshot.

| Layer | Idempotency mechanism | CDC mechanism |
|-------|-----------------------|---------------|
| Bronze | Snapshot model — each load appends a new batch; `cleanup_bronze_snapshots` removes old batches after each run, keeping only the latest snapshot per `_source_file` | S3 `LastModified > pipeline_last_run_ts` (timestamp); short-circuits if nothing new |
| Silver | `ON CONFLICT (order_id) DO UPDATE` | `_ingested_at = MAX(_ingested_at) per _source_file` — always reads the latest Bronze snapshot |
| Gold dims | `ON CONFLICT DO UPDATE` (Type 1) or SCD2 `UPDATE+INSERT` | Full silver table scan |
| Gold fact | `ON CONFLICT (order_id) DO UPDATE` | Full silver table scan |

The pipeline is **safe to re-run any number of times** without duplicating data.

---

## Security Notes

- No credentials exist anywhere in the project folder — not in code, SQL, Docker Compose, or any config file.
- All connection credentials (AWS, PostgreSQL) are entered via the Airflow UI and stored encrypted in Airflow's metadata DB.
- `docker-compose.dev-db.yaml` (contains local DWH password) is git-ignored and never committed.
- `.env` contains only `AIRFLOW_UID` — not sensitive, but git-ignored by convention.
- For production, use IAM Roles or the Airflow Secrets Backend (see [Enterprise security recommendations](#enterprise-security-recommendations)).

---

## Generating Sample Source Data

A script is provided to generate realistic, intentionally messy CSV files for testing:

```bash
# Install dependencies (outside Docker)
pip install pandas numpy

# Generate ~12,000 rows with injected data quality issues
python generate_source_data.py

# Output: data/hardware_sales_YYYYMMDD.csv

# Upload to S3
aws s3 cp data/hardware_sales_YYYYMMDD.csv s3://<your-bucket>/computer_hardware_sales/
```

**Injected data issues** (documented in the script's header):
- Duplicate rows (same `order_id` 2–3 times)
- NULL in key columns (`order_id`, `customer_id`, `product_id`)
- Multiple date formats (`YYYY-MM-DD`, `MM/DD/YYYY`, `DD-Mon-YYYY`)
- Inconsistent casing (`CONSUMER` vs `consumer` vs `Consumer`)
- Whitespace padding (`"  Laptop  "`)
- Negative quantity and price values
- Revenue calculation errors (doesn't match unit_price × quantity)
- Comma-formatted numbers (`"1,250.00"`)

These issues are intentionally handled by the Silver SQL transformation.

---

## Data Dictionary

Full column-level documentation for all schemas and tables is in:

**[docs/data_dictionary.md](./docs/data_dictionary.md)**

Covers: column names, data types, nullable status, business descriptions, join keys, SCD2 reference, and lineage diagram.
