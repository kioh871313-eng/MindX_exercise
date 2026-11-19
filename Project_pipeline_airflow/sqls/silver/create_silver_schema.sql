-- =============================================================
-- Silver Layer Schema
-- Purpose : Cleaned, validated, and type-cast version of the
--           Bronze data. One row per order_id (deduplicated).
--           Serves as the single source of truth for Gold builds.
-- =============================================================

CREATE SCHEMA IF NOT EXISTS silver;

-- -------------------------------------------------------------
-- silver.clean_hardware_sales
-- Properly typed, deduplicated, and validated sales records.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.clean_hardware_sales (
    -- Surrogate key
    id                  BIGSERIAL           PRIMARY KEY,

    -- Order details
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

    -- Metadata
    _source_file        TEXT,
    _bronze_id          BIGINT,             -- FK reference to bronze row
    _loaded_at          TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    -- Business key uniqueness (deduplication)
    CONSTRAINT uq_silver_order_id UNIQUE (order_id)
);

-- Indexes for downstream Gold queries
CREATE INDEX IF NOT EXISTS idx_silver_order_date
    ON silver.clean_hardware_sales (order_date);

CREATE INDEX IF NOT EXISTS idx_silver_customer_id
    ON silver.clean_hardware_sales (customer_id);

CREATE INDEX IF NOT EXISTS idx_silver_product_id
    ON silver.clean_hardware_sales (product_id);

CREATE INDEX IF NOT EXISTS idx_silver_region
    ON silver.clean_hardware_sales (region);

COMMENT ON TABLE silver.clean_hardware_sales IS
    'Cleaned and validated Computer Hardware Sales data. '
    'All types are properly cast, NULLs handled, duplicates removed. '
    'One row per order_id.';
