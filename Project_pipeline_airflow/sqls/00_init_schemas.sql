-- =============================================================
-- 00_init_schemas.sql
-- Auto-executed by the postgres-dwh container on FIRST start only
-- (placed in /docker-entrypoint-initdb.d/ via Docker volume mount).
--
-- Creates all schemas and tables in dependency order.
-- All statements are idempotent (IF NOT EXISTS).
-- =============================================================


-- ================================================================
-- 1. LOGS SCHEMA  (must exist first — DAGs write here from task 1)
-- ================================================================

CREATE SCHEMA IF NOT EXISTS logs;

CREATE TABLE IF NOT EXISTS logs.pipeline_log (
    id              BIGSERIAL       PRIMARY KEY,
    dag_id          TEXT            NOT NULL,
    task_id         TEXT            NOT NULL,
    run_id          TEXT            NOT NULL,
    layer           TEXT            NOT NULL
                        CHECK (layer IN ('bronze', 'silver', 'gold', 'general')),
    status          TEXT            NOT NULL
                        CHECK (status IN ('started', 'success', 'failed')),
    rows_processed  INTEGER         NOT NULL DEFAULT 0,
    error_message   TEXT,
    logged_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_log_dag
    ON logs.pipeline_log (dag_id, logged_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_log_layer
    ON logs.pipeline_log (layer, status, logged_at DESC);

COMMENT ON TABLE logs.pipeline_log IS
    'Pipeline execution log. One row per task event (started / success / failed).';


-- ================================================================
-- 2. BRONZE SCHEMA
-- ================================================================

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.raw_hardware_sales (
    id                  BIGSERIAL       PRIMARY KEY,

    -- Source columns (all TEXT — raw layer, no silent data loss)
    order_id            TEXT,
    order_date          TEXT,
    ship_date           TEXT,
    ship_mode           TEXT,
    customer_id         TEXT,
    customer_name       TEXT,
    customer_segment    TEXT,
    country             TEXT,
    city                TEXT,
    state               TEXT,
    postal_code         TEXT,
    region              TEXT,
    product_id          TEXT,
    category            TEXT,
    sub_category        TEXT,
    product_name        TEXT,
    brand               TEXT,
    unit_price          TEXT,
    unit_cost           TEXT,
    quantity            TEXT,
    discount            TEXT,
    total_revenue       TEXT,
    total_cost          TEXT,
    profit              TEXT,
    sales_channel       TEXT,
    order_status        TEXT,

    -- Metadata
    _source_file        TEXT            NOT NULL,
    _ingested_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Idempotency: same order_id from the same file is upserted, not duplicated.
    -- Supports repeated loads when the S3 file is overwritten with new rows.
    CONSTRAINT uq_bronze_order_file
        UNIQUE (order_id, _source_file)
);

CREATE INDEX IF NOT EXISTS idx_bronze_ingested_at
    ON bronze.raw_hardware_sales (_ingested_at);

CREATE INDEX IF NOT EXISTS idx_bronze_order_id
    ON bronze.raw_hardware_sales (order_id);

COMMENT ON TABLE bronze.raw_hardware_sales IS
    'Raw Computer Hardware Sales data loaded from Amazon S3. '
    'No transformations applied. Retained ≥ 7 days for audit.';


-- ================================================================
-- 3. SILVER SCHEMA
-- ================================================================

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.clean_hardware_sales (
    id                  BIGSERIAL           PRIMARY KEY,

    -- Order
    order_id            TEXT                NOT NULL,
    order_date          DATE,
    ship_date           DATE,
    ship_mode           TEXT,
    order_status        TEXT,
    sales_channel       TEXT,

    -- Customer
    customer_id         TEXT,
    customer_name       TEXT,
    customer_segment    TEXT,

    -- Geography
    country             TEXT,
    city                TEXT,
    state               TEXT,
    postal_code         TEXT,
    region              TEXT,

    -- Product
    product_id          TEXT,
    product_name        TEXT,
    category            TEXT,
    sub_category        TEXT,
    brand               TEXT,

    -- Financials
    unit_price          NUMERIC(12, 2),
    unit_cost           NUMERIC(12, 2),
    quantity            INTEGER,
    discount            NUMERIC(5, 4)       DEFAULT 0,
    total_revenue       NUMERIC(14, 2),
    total_cost          NUMERIC(14, 2),
    profit              NUMERIC(14, 2),

    -- Lineage
    _source_file        TEXT,
    _bronze_id          BIGINT,
    _loaded_at          TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_silver_order_id UNIQUE (order_id)
);

CREATE INDEX IF NOT EXISTS idx_silver_order_date
    ON silver.clean_hardware_sales (order_date);

CREATE INDEX IF NOT EXISTS idx_silver_customer_id
    ON silver.clean_hardware_sales (customer_id);

CREATE INDEX IF NOT EXISTS idx_silver_product_id
    ON silver.clean_hardware_sales (product_id);

COMMENT ON TABLE silver.clean_hardware_sales IS
    'Cleaned and validated sales data. One row per order_id.';


-- ================================================================
-- 4. GOLD SCHEMA — Star Schema
-- ================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- ---- dim_date ------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_id         INTEGER         PRIMARY KEY,
    date            DATE            NOT NULL UNIQUE,
    year            SMALLINT        NOT NULL,
    quarter         SMALLINT        NOT NULL,
    month           SMALLINT        NOT NULL,
    month_name      TEXT            NOT NULL,
    week            SMALLINT        NOT NULL,
    day_of_week     SMALLINT        NOT NULL,
    day_name        TEXT            NOT NULL,
    is_weekend      BOOLEAN         NOT NULL DEFAULT FALSE
);

COMMENT ON TABLE gold.dim_date IS 'Date dimension. Rebuilt daily (Type 1).';


-- ---- dim_geography -------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_geography (
    geo_id          BIGSERIAL       PRIMARY KEY,
    country         TEXT,
    region          TEXT,
    state           TEXT,
    city            TEXT,
    postal_code     TEXT,
    CONSTRAINT uq_geo UNIQUE (country, state, city, postal_code)
);

COMMENT ON TABLE gold.dim_geography IS 'Geography dimension (Type 1).';


-- ---- dim_product (SCD2) --------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_product (
    dim_product_sk  BIGSERIAL       PRIMARY KEY,
    product_id      TEXT            NOT NULL,
    product_name    TEXT,
    category        TEXT,
    sub_category    TEXT,
    brand           TEXT,
    unit_cost       NUMERIC(12, 2),
    unit_price      NUMERIC(12, 2),
    -- SCD2 control
    valid_from      DATE            NOT NULL,
    valid_to        DATE,
    is_current      BOOLEAN         NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_dim_product_id
    ON gold.dim_product (product_id);

CREATE INDEX IF NOT EXISTS idx_dim_product_current
    ON gold.dim_product (product_id, is_current);

COMMENT ON TABLE gold.dim_product IS
    'Product dimension (SCD Type 2). Use is_current=TRUE for latest version.';


-- ---- dim_customer (SCD2) -------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    dim_customer_sk BIGSERIAL       PRIMARY KEY,
    customer_id     TEXT            NOT NULL,
    customer_name   TEXT,
    customer_segment TEXT,
    country         TEXT,
    city            TEXT,
    region          TEXT,
    -- SCD2 control
    valid_from      DATE            NOT NULL,
    valid_to        DATE,
    is_current      BOOLEAN         NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_id
    ON gold.dim_customer (customer_id);

CREATE INDEX IF NOT EXISTS idx_dim_customer_current
    ON gold.dim_customer (customer_id, is_current);

COMMENT ON TABLE gold.dim_customer IS
    'Customer dimension (SCD Type 2). Use is_current=TRUE for latest version.';


-- ---- fact_sales ----------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_sales (
    fact_id             BIGSERIAL       PRIMARY KEY,
    order_id            TEXT            NOT NULL UNIQUE,

    -- Foreign keys to dimensions
    order_date_id       INTEGER         REFERENCES gold.dim_date (date_id),
    ship_date_id        INTEGER         REFERENCES gold.dim_date (date_id),
    dim_customer_sk     BIGINT          REFERENCES gold.dim_customer (dim_customer_sk),
    dim_product_sk      BIGINT          REFERENCES gold.dim_product (dim_product_sk),
    geo_id              BIGINT          REFERENCES gold.dim_geography (geo_id),

    -- Degenerate dimensions
    ship_mode           TEXT,
    sales_channel       TEXT,
    order_status        TEXT,

    -- Measures
    quantity            INTEGER,
    unit_price          NUMERIC(12, 2),
    unit_cost           NUMERIC(12, 2),
    discount            NUMERIC(5, 4),
    total_revenue       NUMERIC(14, 2),
    total_cost          NUMERIC(14, 2),
    profit              NUMERIC(14, 2),

    _loaded_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_order_date
    ON gold.fact_sales (order_date_id);

CREATE INDEX IF NOT EXISTS idx_fact_customer
    ON gold.fact_sales (dim_customer_sk);

CREATE INDEX IF NOT EXISTS idx_fact_product
    ON gold.fact_sales (dim_product_sk);

COMMENT ON TABLE gold.fact_sales IS
    'Fact table. Grain: one row per order_id. '
    'Join dims via surrogate keys. Use is_current=TRUE for current dim values.';
