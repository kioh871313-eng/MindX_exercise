-- =============================================================
-- Gold Layer Schema — Star Schema Data Mart
-- Purpose : Dimensional model (star schema) optimised for
--           reporting and analytics.
--
-- SCD2 strategy:
--   Applied to dim_product and dim_customer.
--   Control columns: valid_from, valid_to, is_current.
--   Best-practice note: SCD2 is applied at the Gold layer only.
--   Bronze and Silver store current-state data; historical versions
--   live exclusively in Gold dimensions. This keeps the pipeline
--   simple and auditable — Bronze/Silver are immutable raw/clean
--   stores, while Gold handles slowly-changing business attributes.
-- =============================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- -------------------------------------------------------------
-- gold.dim_date
-- Date dimension — Type 1 (full rebuild daily).
-- No SCD2 needed: calendar attributes do not change.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_id         INTEGER         PRIMARY KEY,   -- YYYYMMDD
    date            DATE            NOT NULL UNIQUE,
    year            SMALLINT        NOT NULL,
    quarter         SMALLINT        NOT NULL,
    month           SMALLINT        NOT NULL,
    month_name      TEXT            NOT NULL,
    week            SMALLINT        NOT NULL,
    day_of_week     SMALLINT        NOT NULL,      -- 0 = Monday
    day_name        TEXT            NOT NULL,
    is_weekend      BOOLEAN         NOT NULL DEFAULT FALSE
);

COMMENT ON TABLE gold.dim_date IS 'Date dimension. Rebuilt daily. No SCD2.';


-- -------------------------------------------------------------
-- gold.dim_geography
-- Geography dimension — Type 1 (full rebuild daily).
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_geography (
    geo_id          BIGSERIAL       PRIMARY KEY,
    country         TEXT,
    region          TEXT,
    state           TEXT,
    city            TEXT,
    postal_code     TEXT,
    CONSTRAINT uq_geo UNIQUE (country, state, city, postal_code)
);

COMMENT ON TABLE gold.dim_geography IS 'Geography dimension. Rebuilt daily. No SCD2.';


-- -------------------------------------------------------------
-- gold.dim_product
-- Product dimension — SCD2 (tracks price / category changes).
-- valid_to IS NULL means the record is currently active.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_product (
    dim_product_sk  BIGSERIAL       PRIMARY KEY,   -- Surrogate key

    -- Natural key
    product_id      TEXT            NOT NULL,

    -- Tracked attributes (changes trigger new SCD2 version)
    product_name    TEXT,
    category        TEXT,
    sub_category    TEXT,
    brand           TEXT,
    unit_cost       NUMERIC(12, 2),
    unit_price      NUMERIC(12, 2),

    -- SCD2 control columns
    valid_from      DATE            NOT NULL,
    valid_to        DATE,                          -- NULL = currently active
    is_current      BOOLEAN         NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_dim_product_id
    ON gold.dim_product (product_id);

CREATE INDEX IF NOT EXISTS idx_dim_product_current
    ON gold.dim_product (product_id, is_current);

COMMENT ON TABLE gold.dim_product IS
    'Product dimension with SCD Type 2. '
    'Use is_current = TRUE to get the latest version.';


-- -------------------------------------------------------------
-- gold.dim_customer
-- Customer dimension — SCD2 (tracks segment / location changes).
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    dim_customer_sk BIGSERIAL       PRIMARY KEY,

    -- Natural key
    customer_id     TEXT            NOT NULL,

    -- Tracked attributes
    customer_name   TEXT,
    customer_segment TEXT,
    country         TEXT,
    city            TEXT,
    region          TEXT,

    -- SCD2 control columns
    valid_from      DATE            NOT NULL,
    valid_to        DATE,
    is_current      BOOLEAN         NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_id
    ON gold.dim_customer (customer_id);

CREATE INDEX IF NOT EXISTS idx_dim_customer_current
    ON gold.dim_customer (customer_id, is_current);

COMMENT ON TABLE gold.dim_customer IS
    'Customer dimension with SCD Type 2. '
    'Use is_current = TRUE to get the latest version.';


-- -------------------------------------------------------------
-- gold.fact_sales
-- Central fact table — grain: one row per order.
-- Uses surrogate keys (SK) to join dimensions.
-- FK to dim_product / dim_customer points at current version (is_current=TRUE)
-- for fast queries; for point-in-time analysis join on valid_from/valid_to.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_sales (
    fact_id             BIGSERIAL       PRIMARY KEY,

    -- Business key (idempotency)
    order_id            TEXT            NOT NULL UNIQUE,

    -- Foreign keys
    order_date_id       INTEGER         REFERENCES gold.dim_date (date_id),
    ship_date_id        INTEGER         REFERENCES gold.dim_date (date_id),
    dim_customer_sk     BIGINT          REFERENCES gold.dim_customer (dim_customer_sk),
    dim_product_sk      BIGINT          REFERENCES gold.dim_product (dim_product_sk),
    geo_id              BIGINT          REFERENCES gold.dim_geography (geo_id),

    -- Degenerate dimensions (kept on fact for convenience)
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

    -- Metadata
    _loaded_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_order_date
    ON gold.fact_sales (order_date_id);

CREATE INDEX IF NOT EXISTS idx_fact_customer
    ON gold.fact_sales (dim_customer_sk);

CREATE INDEX IF NOT EXISTS idx_fact_product
    ON gold.fact_sales (dim_product_sk);

COMMENT ON TABLE gold.fact_sales IS
    'Central fact table for Computer Hardware Sales. '
    'Grain: one row per order_id. '
    'Join dims on SK columns; use is_current=TRUE for latest dimension values.';
