-- =============================================================
-- Bronze Layer Schema
-- Purpose : Store raw, unmodified data as extracted from S3.
--           Supports on-demand / hourly loads where the same
--           S3 file can be overwritten with new rows appended.
--           When the same order_id appears again from the same
--           source file (file was updated), the row is overwritten
--           with the latest values (ON CONFLICT DO UPDATE).
-- =============================================================

CREATE SCHEMA IF NOT EXISTS bronze;

-- -------------------------------------------------------------
-- bronze.raw_hardware_sales
-- Unique key: (order_id, _source_file)
--   - Allows the same order to exist from different source files.
--   - When a file is overwritten on S3 and re-loaded, existing
--     rows are updated rather than duplicated.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_hardware_sales (
    id                  BIGSERIAL       PRIMARY KEY,

    -- Source business columns (all TEXT — raw layer, no type coercion)
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

    -- Idempotency: same order_id from the same file is upserted,
    -- not duplicated. Supports repeated loads from the same S3 file.
    CONSTRAINT uq_bronze_order_file
        UNIQUE (order_id, _source_file)
);

CREATE INDEX IF NOT EXISTS idx_bronze_ingested_at
    ON bronze.raw_hardware_sales (_ingested_at);

CREATE INDEX IF NOT EXISTS idx_bronze_order_id
    ON bronze.raw_hardware_sales (order_id);

COMMENT ON TABLE bronze.raw_hardware_sales IS
    'Raw Computer Hardware Sales data loaded from Amazon S3. '
    'No transformations applied. '
    'Supports repeated loads from the same file via upsert on (order_id, _source_file).';
