# Data Dictionary — Computer Hardware Sales DWH

This document describes every schema, table, and column in the data warehouse.
The warehouse uses a **Medallion architecture** (Bronze → Silver → Gold) hosted on PostgreSQL.

---

## Table of Contents

1. [Schema Overview](#schema-overview)
2. [Schema: `logs`](#schema-logs)
   - [logs.pipeline_log](#logspipeline_log)
3. [Schema: `bronze`](#schema-bronze)
   - [bronze.raw_hardware_sales](#bronzeraw_hardware_sales)
4. [Schema: `silver`](#schema-silver)
   - [silver.clean_hardware_sales](#silverclean_hardware_sales)
5. [Schema: `gold`](#schema-gold)
   - [gold.dim_date](#golddim_date)
   - [gold.dim_geography](#golddim_geography)
   - [gold.dim_product](#golddim_product)
   - [gold.dim_customer](#golddim_customer)
   - [gold.fact_sales](#goldfact_sales)
6. [Star Schema Diagram](#star-schema-diagram)
7. [SCD2 Reference](#scd2-reference)
8. [Data Lineage](#data-lineage)

---

## Schema Overview

| Schema   | Purpose                                        | Tables                                                                 |
|----------|------------------------------------------------|------------------------------------------------------------------------|
| `logs`   | Pipeline execution audit trail                 | `pipeline_log`                                                         |
| `bronze` | Raw, unmodified data from S3 (all TEXT)        | `raw_hardware_sales`                                                   |
| `silver` | Cleaned, typed, deduplicated current state     | `clean_hardware_sales`                                                 |
| `gold`   | Star-schema data mart for reporting            | `dim_date`, `dim_geography`, `dim_product`, `dim_customer`, `fact_sales` |

---

## Schema: `logs`

### logs.pipeline_log

Centralised audit log. Every DAG task writes one row per event (started / success / failed).
Use this table to monitor pipeline health and trace errors back to a specific run.

| Column          | Type         | Nullable | Description                                                                 |
|-----------------|--------------|----------|-----------------------------------------------------------------------------|
| `id`            | BIGSERIAL    | NOT NULL | Auto-incrementing surrogate key.                                            |
| `dag_id`        | TEXT         | NOT NULL | Airflow DAG identifier. `hardware_sales_pipeline` for all pipeline tasks. |
| `task_id`       | TEXT         | NOT NULL | Airflow Task identifier within the DAG.                                     |
| `run_id`        | TEXT         | NOT NULL | Airflow DAG Run ID. Unique per execution.                                   |
| `layer`         | TEXT         | NOT NULL | Medallion layer. One of: `bronze`, `silver`, `gold`, `general`.             |
| `status`        | TEXT         | NOT NULL | Execution status. One of: `started`, `success`, `failed`.                  |
| `rows_processed`| INTEGER      | NOT NULL | Number of rows inserted / updated by the task. `0` for non-data tasks.     |
| `error_message` | TEXT         | NULL     | Populated only when `status = 'failed'`. Contains the exception message.   |
| `logged_at`     | TIMESTAMPTZ  | NOT NULL | UTC timestamp when the log entry was written. Defaults to `NOW()`.         |

**Indexes:** `(dag_id, logged_at DESC)`, `(layer, status, logged_at DESC)`

**Example query — last 24 h failures:**
```sql
SELECT dag_id, task_id, layer, error_message, logged_at
FROM logs.pipeline_log
WHERE status = 'failed'
  AND logged_at >= NOW() - INTERVAL '24 hours'
ORDER BY logged_at DESC;
```

---

## Schema: `bronze`

Stores raw data exactly as extracted from S3 CSV files.
No transformations are applied. All business columns are stored as `TEXT` to prevent silent data loss from type coercion errors.
Rows are retained indefinitely for audit / trace-back purposes.

### bronze.raw_hardware_sales

| Column            | Type        | Nullable | Description                                                                                   |
|-------------------|-------------|----------|-----------------------------------------------------------------------------------------------|
| `id`              | BIGSERIAL   | NOT NULL | Auto-incrementing surrogate key. Used as CDC cursor by the Silver DAG.                        |
| `order_id`        | TEXT        | NULL     | Source order identifier. May be NULL or duplicated in raw data (intentional data quality issue). |
| `order_date`      | TEXT        | NULL     | Order date as-is from source. May be in multiple formats: `YYYY-MM-DD`, `MM/DD/YYYY`, `DD-Mon-YYYY`, `DD/MM/YYYY`. |
| `ship_date`       | TEXT        | NULL     | Shipment date. Same format variability as `order_date`.                                       |
| `ship_mode`       | TEXT        | NULL     | Shipping method. E.g. `Standard Class`, `First Class`, `Same Day`.                            |
| `customer_id`     | TEXT        | NULL     | Customer identifier. May be NULL in source.                                                   |
| `customer_name`   | TEXT        | NULL     | Full name of the customer. May have leading/trailing whitespace.                              |
| `customer_segment`| TEXT        | NULL     | Market segment. E.g. `Consumer`, `Corporate`. Casing may be inconsistent.                    |
| `country`         | TEXT        | NULL     | Country name.                                                                                 |
| `city`            | TEXT        | NULL     | City name.                                                                                    |
| `state`           | TEXT        | NULL     | State or province.                                                                            |
| `postal_code`     | TEXT        | NULL     | Postal / ZIP code.                                                                            |
| `region`          | TEXT        | NULL     | Sales region. E.g. `North America`, `Europe`. May be NULL or empty.                          |
| `product_id`      | TEXT        | NULL     | Product identifier. May be NULL in source.                                                    |
| `category`        | TEXT        | NULL     | Product category. E.g. `Laptops`, `Storage`, `Monitors`.                                     |
| `sub_category`    | TEXT        | NULL     | Product sub-category. E.g. `Gaming Laptops`, `SSD NVMe`.                                     |
| `product_name`    | TEXT        | NULL     | Full product name. May have whitespace padding.                                               |
| `brand`           | TEXT        | NULL     | Brand name. E.g. `Dell`, `Samsung`, `Corsair`.                                               |
| `unit_price`      | TEXT        | NULL     | Sale price per unit. May be formatted as `"1,250.00"` (with comma). Negative values possible. |
| `unit_cost`       | TEXT        | NULL     | Cost per unit. Same formatting issues as `unit_price`.                                        |
| `quantity`        | TEXT        | NULL     | Units sold. Stored as TEXT; may be negative or non-numeric.                                   |
| `discount`        | TEXT        | NULL     | Discount fraction (0–1). E.g. `0.1` = 10% discount.                                         |
| `total_revenue`   | TEXT        | NULL     | Pre-calculated revenue from source. **Not trusted** — Silver recalculates this.               |
| `total_cost`      | TEXT        | NULL     | Pre-calculated cost from source. **Not trusted** — Silver recalculates this.                  |
| `profit`          | TEXT        | NULL     | Pre-calculated profit from source. **Not trusted** — Silver recalculates this.                |
| `sales_channel`   | TEXT        | NULL     | Sales channel. E.g. `Online`, `Retail Store`, `Reseller`.                                    |
| `order_status`    | TEXT        | NULL     | Order status. E.g. `Completed`, `Shipped`, `Returned`.                                       |
| `_source_file`    | TEXT        | NOT NULL | S3 object key of the CSV file this row was loaded from.                                      |
| `_ingested_at`    | TIMESTAMPTZ | NOT NULL | UTC timestamp when this row was loaded into bronze. Defaults to `NOW()`.                     |

**Unique constraint:** `(order_id, _source_file)` — when the same S3 file is re-loaded
(e.g. file was overwritten with new rows), existing rows are updated and new rows are
inserted. This supports multiple pipeline runs per day against the same file.

**Indexes:** `(_ingested_at)`, `(order_id)`

---

## Schema: `silver`

Cleaned, type-cast, and deduplicated version of bronze data.
One row per `order_id` (business key). This is the single source of truth that feeds all Gold tables.

All transformations are defined in `sqls/silver/transform_bronze_to_silver.sql`.

### silver.clean_hardware_sales

| Column            | Type           | Nullable | Description                                                                                           |
|-------------------|----------------|----------|-------------------------------------------------------------------------------------------------------|
| `id`              | BIGSERIAL      | NOT NULL | Auto-incrementing surrogate key.                                                                      |
| `order_id`        | TEXT           | NOT NULL | Cleaned order identifier (trimmed). Business key — unique in this table.                             |
| `order_date`      | DATE           | NULL     | Parsed order date. NULL if source value could not be parsed to any known format.                      |
| `ship_date`       | DATE           | NULL     | Parsed ship date. NULL if unparseable.                                                                |
| `ship_mode`       | TEXT           | NULL     | Normalised to `INITCAP` casing. E.g. `First Class`.                                                 |
| `order_status`    | TEXT           | NULL     | Normalised to `INITCAP`. E.g. `Completed`, `Returned`.                                              |
| `sales_channel`   | TEXT           | NULL     | Normalised to `INITCAP`. E.g. `Online`, `Retail Store`.                                             |
| `customer_id`     | TEXT           | NULL     | Trimmed customer identifier.                                                                          |
| `customer_name`   | TEXT           | NULL     | Normalised to `INITCAP`, whitespace trimmed.                                                         |
| `customer_segment`| TEXT           | NULL     | Normalised to `INITCAP`. E.g. `Consumer`, `Corporate`, `Small Business`.                            |
| `country`         | TEXT           | NULL     | Normalised to `INITCAP`.                                                                              |
| `city`            | TEXT           | NULL     | Normalised to `INITCAP`.                                                                              |
| `state`           | TEXT           | NULL     | Normalised to `INITCAP`.                                                                              |
| `postal_code`     | TEXT           | NULL     | Trimmed. Kept as TEXT to preserve leading zeros.                                                     |
| `region`          | TEXT           | NULL     | Trimmed. Empty string or NULL from source replaced with `'Unknown'`.                                 |
| `product_id`      | TEXT           | NULL     | Trimmed product identifier.                                                                           |
| `product_name`    | TEXT           | NULL     | Normalised to `INITCAP`, whitespace trimmed.                                                         |
| `category`        | TEXT           | NULL     | Normalised to `INITCAP`. E.g. `Laptops`, `Storage`.                                                 |
| `sub_category`    | TEXT           | NULL     | Normalised to `INITCAP`. E.g. `Gaming Laptops`.                                                     |
| `brand`           | TEXT           | NULL     | Normalised to `INITCAP`. E.g. `Dell`, `Samsung`.                                                    |
| `unit_price`      | NUMERIC(12,2)  | NULL     | Comma-stripped, cast to NUMERIC. NULL if negative or non-numeric.                                   |
| `unit_cost`       | NUMERIC(12,2)  | NULL     | Comma-stripped, cast to NUMERIC. NULL if negative or non-numeric.                                   |
| `quantity`        | INTEGER        | NULL     | Cast to INTEGER. NULL if negative or non-numeric.                                                    |
| `discount`        | NUMERIC(5,4)   | NOT NULL | Discount fraction (0–1). NULL from source replaced with `0`.                                        |
| `total_revenue`   | NUMERIC(14,2)  | NULL     | **Recalculated** as `unit_price × quantity × (1 − discount)`. Source value ignored.                 |
| `total_cost`      | NUMERIC(14,2)  | NULL     | **Recalculated** as `unit_cost × quantity`. Source value ignored.                                   |
| `profit`          | NUMERIC(14,2)  | NULL     | **Recalculated** as `total_revenue − total_cost`. Source value ignored.                             |
| `_source_file`    | TEXT           | NULL     | Inherited from bronze — S3 key of the originating CSV file.                                         |
| `_bronze_id`      | BIGINT         | NULL     | FK reference to `bronze.raw_hardware_sales.id`. Enables row-level lineage tracing.                  |
| `_loaded_at`      | TIMESTAMPTZ    | NOT NULL | UTC timestamp when this row was written to silver. Defaults to `NOW()`.                              |

**Unique constraint:** `(order_id)`

**Indexes:** `(order_date)`, `(customer_id)`, `(product_id)`

> **Note on measures:** `total_revenue`, `total_cost`, and `profit` are always recalculated from `unit_price`, `unit_cost`, `quantity`, and `discount` in the Silver SQL. Source-provided values are discarded. This prevents calculation errors from propagating downstream.

---

## Schema: `gold`

Star-schema data mart. Optimised for reporting and analytical queries.
All tables are populated by SQL transformation files in `sqls/gold/`.

### gold.dim_date

Calendar dimension. Rebuilt daily using all unique dates from `silver.clean_hardware_sales`.
**Type 1** — no history (calendar attributes are immutable).

| Column        | Type      | Nullable | Description                                                                   |
|---------------|-----------|----------|-------------------------------------------------------------------------------|
| `date_id`     | INTEGER   | NOT NULL | Primary key. Integer in `YYYYMMDD` format. E.g. `20240315` for 2024-03-15.  |
| `date`        | DATE      | NOT NULL | Calendar date. Unique.                                                        |
| `year`        | SMALLINT  | NOT NULL | Calendar year. E.g. `2024`.                                                   |
| `quarter`     | SMALLINT  | NOT NULL | Calendar quarter (1–4).                                                       |
| `month`       | SMALLINT  | NOT NULL | Calendar month (1–12).                                                        |
| `month_name`  | TEXT      | NOT NULL | Full month name. E.g. `March`.                                               |
| `week`        | SMALLINT  | NOT NULL | ISO week number (1–53).                                                       |
| `day_of_week` | SMALLINT  | NOT NULL | Day of week (0 = Sunday, 6 = Saturday). PostgreSQL `EXTRACT(DOW)` convention.|
| `day_name`    | TEXT      | NOT NULL | Full day name. E.g. `Monday`.                                                |
| `is_weekend`  | BOOLEAN   | NOT NULL | `TRUE` if `day_of_week` is 0 (Sunday) or 6 (Saturday).                      |

**Join to fact:** `fact_sales.order_date_id = dim_date.date_id`

---

### gold.dim_geography

Geography dimension. Upserted daily from silver data.
**Type 1** — no SCD2 (geography hierarchy does not change).

| Column        | Type      | Nullable | Description                                              |
|---------------|-----------|----------|----------------------------------------------------------|
| `geo_id`      | BIGSERIAL | NOT NULL | Surrogate key (auto-generated).                          |
| `country`     | TEXT      | NULL     | Country name. E.g. `United States`, `Germany`.          |
| `region`      | TEXT      | NULL     | Sales region. E.g. `North America`, `Europe`.           |
| `state`       | TEXT      | NULL     | State or province. E.g. `California`, `Bavaria`.        |
| `city`        | TEXT      | NULL     | City name. E.g. `San Francisco`.                        |
| `postal_code` | TEXT      | NULL     | Postal / ZIP code. Kept as TEXT for leading zeros.      |

**Unique constraint:** `(country, state, city, postal_code)`

**Join to fact:** `fact_sales.geo_id = dim_geography.geo_id`

---

### gold.dim_product

Product dimension with **SCD Type 2** history tracking.
A new row is inserted whenever a tracked attribute changes, preserving the full history of product pricing and categorisation changes.

| Column          | Type           | Nullable | Description                                                                            |
|-----------------|----------------|----------|----------------------------------------------------------------------------------------|
| `dim_product_sk`| BIGSERIAL      | NOT NULL | **Surrogate key** — primary key. Use this to join to `fact_sales`.                    |
| `product_id`    | TEXT           | NOT NULL | **Natural key** — business identifier from source. Not unique in this table (multiple versions exist). |
| `product_name`  | TEXT           | NULL     | Product name. Changes here trigger a new SCD2 version.                               |
| `category`      | TEXT           | NULL     | Product category. E.g. `Laptops`, `Monitors`.                                        |
| `sub_category`  | TEXT           | NULL     | Product sub-category. E.g. `Gaming Laptops`.                                         |
| `brand`         | TEXT           | NULL     | Brand name. E.g. `Dell`, `NVIDIA`.                                                   |
| `unit_cost`     | NUMERIC(12,2)  | NULL     | Cost price. Changes trigger a new SCD2 version.                                      |
| `unit_price`    | NUMERIC(12,2)  | NULL     | Selling price. Changes trigger a new SCD2 version.                                   |
| `valid_from`    | DATE           | NOT NULL | Date this version became active.                                                      |
| `valid_to`      | DATE           | NULL     | Date this version was superseded. `NULL` means this is the current active version.   |
| `is_current`    | BOOLEAN        | NOT NULL | `TRUE` for the latest active version. Use `WHERE is_current = TRUE` for current state.|

**Indexes:** `(product_id)`, `(product_id, is_current)`

**Tracked columns (trigger SCD2):** `product_name`, `category`, `sub_category`, `brand`, `unit_cost`, `unit_price`

**Join to fact (current state):** `fact_sales.dim_product_sk = dim_product.dim_product_sk`

**Point-in-time join:**
```sql
SELECT p.*
FROM gold.fact_sales f
JOIN gold.dim_product p
  ON  p.product_id  = /* natural key lookup */
  AND p.valid_from <= f.order_date::DATE
  AND (p.valid_to   > f.order_date::DATE OR p.valid_to IS NULL);
```

---

### gold.dim_customer

Customer dimension with **SCD Type 2** history tracking.
Tracks changes to customer segment, location, or name over time.

| Column            | Type      | Nullable | Description                                                                                 |
|-------------------|-----------|----------|---------------------------------------------------------------------------------------------|
| `dim_customer_sk` | BIGSERIAL | NOT NULL | **Surrogate key** — primary key.                                                            |
| `customer_id`     | TEXT      | NOT NULL | **Natural key** — business identifier from source. Not unique (multiple SCD2 versions).    |
| `customer_name`   | TEXT      | NULL     | Customer full name. Changes trigger a new SCD2 version.                                    |
| `customer_segment`| TEXT      | NULL     | Market segment. E.g. `Consumer`, `Corporate`, `Enterprise`. Changes trigger SCD2.         |
| `country`         | TEXT      | NULL     | Country of the customer. Changes trigger SCD2.                                             |
| `city`            | TEXT      | NULL     | City. Changes trigger SCD2.                                                                |
| `region`          | TEXT      | NULL     | Sales region. Changes trigger SCD2.                                                        |
| `valid_from`      | DATE      | NOT NULL | Date this version became active.                                                           |
| `valid_to`        | DATE      | NULL     | Date superseded. `NULL` = current active version.                                         |
| `is_current`      | BOOLEAN   | NOT NULL | `TRUE` for the current active version.                                                     |

**Indexes:** `(customer_id)`, `(customer_id, is_current)`

**Tracked columns (trigger SCD2):** `customer_name`, `customer_segment`, `country`, `city`, `region`

---

### gold.fact_sales

Central fact table. **Grain: one row per `order_id`.**
Links every sales transaction to its dimension context via surrogate keys.

| Column            | Type           | Nullable | Description                                                                                               |
|-------------------|----------------|----------|-----------------------------------------------------------------------------------------------------------|
| `fact_id`         | BIGSERIAL      | NOT NULL | Auto-incrementing surrogate key.                                                                          |
| `order_id`        | TEXT           | NOT NULL | Business key. Unique in this table. Matches `silver.clean_hardware_sales.order_id`.                      |
| `order_date_id`   | INTEGER        | NULL     | FK → `dim_date.date_id`. Integer `YYYYMMDD` of the order date.                                           |
| `ship_date_id`    | INTEGER        | NULL     | FK → `dim_date.date_id`. Integer `YYYYMMDD` of the ship date.                                            |
| `dim_customer_sk` | BIGINT         | NULL     | FK → `dim_customer.dim_customer_sk`. Points to the **current** active customer version at load time.     |
| `dim_product_sk`  | BIGINT         | NULL     | FK → `dim_product.dim_product_sk`. Points to the **current** active product version at load time.        |
| `geo_id`          | BIGINT         | NULL     | FK → `dim_geography.geo_id`. Resolved by matching country/state/city/postal_code.                        |
| `ship_mode`       | TEXT           | NULL     | Degenerate dimension. E.g. `Standard Class`, `Same Day`.                                                 |
| `sales_channel`   | TEXT           | NULL     | Degenerate dimension. E.g. `Online`, `Reseller`.                                                         |
| `order_status`    | TEXT           | NULL     | Degenerate dimension. E.g. `Completed`, `Returned`, `Cancelled`.                                        |
| `quantity`        | INTEGER        | NULL     | Units sold.                                                                                               |
| `unit_price`      | NUMERIC(12,2)  | NULL     | Selling price per unit at time of order.                                                                 |
| `unit_cost`       | NUMERIC(12,2)  | NULL     | Cost per unit at time of order.                                                                          |
| `discount`        | NUMERIC(5,4)   | NULL     | Discount fraction applied (0–1).                                                                         |
| `total_revenue`   | NUMERIC(14,2)  | NULL     | `unit_price × quantity × (1 − discount)`. Recalculated in silver.                                       |
| `total_cost`      | NUMERIC(14,2)  | NULL     | `unit_cost × quantity`. Recalculated in silver.                                                          |
| `profit`          | NUMERIC(14,2)  | NULL     | `total_revenue − total_cost`. Recalculated in silver.                                                   |
| `_loaded_at`      | TIMESTAMPTZ    | NOT NULL | UTC timestamp when this row was loaded into the fact table.                                              |

**Indexes:** `(order_date_id)`, `(dim_customer_sk)`, `(dim_product_sk)`

> **NULL surrogate keys:** A NULL `dim_product_sk` or `dim_customer_sk` means the product/customer could not be resolved in the dimension table (data arrived before its dimension record). The Gold validation DAG task checks for this and raises an alert.

---

## Star Schema Diagram

```
                        ┌─────────────────┐
                        │   dim_date      │
                        │─────────────────│
                        │ date_id (PK)    │
                        │ date            │
                        │ year / quarter  │
                        │ month / week    │
                        │ day_of_week     │
                        │ is_weekend      │
                        └────────┬────────┘
                                 │ order_date_id / ship_date_id
                                 │
┌──────────────────┐    ┌────────┴───────────┐    ┌──────────────────────┐
│   dim_customer   │    │     fact_sales      │    │    dim_product       │
│──────────────────│    │─────────────────────│    │──────────────────────│
│ dim_customer_sk  ├────┤ fact_id (PK)        ├────┤ dim_product_sk (PK)  │
│ customer_id      │    │ order_id (UK)       │    │ product_id           │
│ customer_name    │    │ order_date_id  (FK) │    │ product_name         │
│ customer_segment │    │ ship_date_id   (FK) │    │ category             │
│ country / city   │    │ dim_customer_sk(FK) │    │ sub_category         │
│ region           │    │ dim_product_sk (FK) │    │ brand                │
│ valid_from       │    │ geo_id         (FK) │    │ unit_price           │
│ valid_to         │    │ ship_mode           │    │ unit_cost            │
│ is_current       │    │ sales_channel       │    │ valid_from           │
└──────────────────┘    │ order_status        │    │ valid_to             │
                        │ quantity            │    │ is_current           │
                        │ unit_price          │    └──────────────────────┘
                        │ unit_cost           │
                        │ discount            │    ┌──────────────────────┐
                        │ total_revenue       │    │   dim_geography      │
                        │ total_cost          │    │──────────────────────│
                        │ profit              ├────┤ geo_id (PK)          │
                        └─────────────────────┘    │ country              │
                                                   │ region               │
                                                   │ state / city         │
                                                   │ postal_code          │
                                                   └──────────────────────┘
```

---

## SCD2 Reference

SCD Type 2 is applied to `dim_product` and `dim_customer`.

### How it works

| Scenario                  | Action                                                                 |
|---------------------------|------------------------------------------------------------------------|
| New product/customer      | Insert one row: `valid_from = TODAY`, `valid_to = NULL`, `is_current = TRUE` |
| Tracked attribute changed | 1. UPDATE existing row: `valid_to = TODAY`, `is_current = FALSE` <br>2. INSERT new row: `valid_from = TODAY`, `valid_to = NULL`, `is_current = TRUE` |
| No change                 | No action — existing row untouched                                     |

### Which layer holds history?

| Layer  | History? |
|--------|----------|
| Bronze | No — raw current snapshot only                        |
| Silver | No — one row per `order_id`, always latest clean state |
| Gold   | **Yes** — `dim_product` and `dim_customer` store all versions |

SCD2 is applied only at Gold because Bronze and Silver serve different purposes (audit store and clean current state). Mixing history tracking into those layers would complicate CDC and deduplication logic unnecessarily.

---

## Data Lineage

```
S3 CSV files  (same file may be overwritten multiple times per day)
    │
    │  dag_pipeline.py — task: extract_load_bronze
    │  (Python reads CSV → upserts all columns as TEXT
    │   ON CONFLICT (order_id, _source_file) DO UPDATE)
    ▼
bronze.raw_hardware_sales
    │
    │  dag_pipeline.py — task: transform_silver
    │  sqls/silver/transform_bronze_to_silver.sql
    │  (SQL: date parsing, type casting, normalisation,
    │         measure recalculation, CDC incremental load)
    ▼
silver.clean_hardware_sales
    │
    ├──► sqls/gold/transform_silver_to_dim_date.sql      → gold.dim_date
    ├──► sqls/gold/transform_silver_to_dim_geography.sql → gold.dim_geography
    ├──► sqls/gold/transform_silver_to_dim_product.sql   → gold.dim_product   (SCD2)
    ├──► sqls/gold/transform_silver_to_dim_customer.sql  → gold.dim_customer  (SCD2)
    └──► sqls/gold/transform_silver_to_fact_sales.sql    → gold.fact_sales
                                                            (JOINs dims for SK resolution)
```

Each step writes to `logs.pipeline_log`. The CDC checkpoint `pipeline_last_run_ts`
is updated only after the full pipeline succeeds, ensuring failed runs re-process
from the same point on the next attempt.
