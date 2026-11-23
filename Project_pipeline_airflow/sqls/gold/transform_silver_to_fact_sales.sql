-- =============================================================
-- Gold — fact_sales (Grain: one row per order_id)
-- Source : silver.clean_hardware_sales
-- Target : gold.fact_sales
-- Purpose: Build the central fact table by joining silver data
--          to dimension surrogate keys.
--
-- Surrogate key resolution:
--   dim_product_sk  → gold.dim_product  WHERE is_current = TRUE
--   dim_customer_sk → gold.dim_customer WHERE is_current = TRUE
--   geo_id          → gold.dim_geography (natural key join)
--   order_date_id   → gold.dim_date (YYYYMMDD integer)
--   ship_date_id    → gold.dim_date (YYYYMMDD integer)
--
-- Idempotency: INSERT … ON CONFLICT (order_id) DO UPDATE
--
-- Called by: dag_gold_build.py
-- =============================================================

INSERT INTO gold.fact_sales (
    order_id,
    order_date_id,
    ship_date_id,
    dim_customer_sk,
    dim_product_sk,
    geo_id,
    ship_mode,
    sales_channel,
    order_status,
    quantity,
    unit_price,
    unit_cost,
    discount,
    total_revenue,
    total_cost,
    profit,
    _loaded_at
)
SELECT
    s.order_id,

    -- Date surrogate keys (YYYYMMDD integer)
    TO_CHAR(s.order_date, 'YYYYMMDD')::INTEGER          AS order_date_id,
    TO_CHAR(s.ship_date,  'YYYYMMDD')::INTEGER          AS ship_date_id,

    -- Customer surrogate key — current active version
    dc.dim_customer_sk,

    -- Product surrogate key — current active version
    dp.dim_product_sk,

    -- Geography key
    dg.geo_id,

    -- Degenerate dimensions
    s.ship_mode,
    s.sales_channel,
    s.order_status,

    -- Measures
    s.quantity,
    s.unit_price,
    s.unit_cost,
    s.discount,
    s.total_revenue,
    s.total_cost,
    s.profit,

    NOW() AT TIME ZONE 'Asia/Ho_Chi_Minh'                AS _loaded_at

FROM silver.clean_hardware_sales s

-- Resolve product surrogate key (current version only)
LEFT JOIN gold.dim_product dp
    ON  dp.product_id  = s.product_id
    AND dp.is_current  = TRUE

-- Resolve customer surrogate key (current version only)
LEFT JOIN gold.dim_customer dc
    ON  dc.customer_id  = s.customer_id
    AND dc.is_current   = TRUE

-- Resolve geography key
LEFT JOIN gold.dim_geography dg
    ON  dg.country     = s.country
    AND dg.state       = s.state
    AND dg.city        = s.city
    AND dg.postal_code = s.postal_code

-- Only process rows not yet in fact (new orders)
-- plus update existing rows that may have changed in silver
WHERE s.order_id IS NOT NULL

ON CONFLICT (order_id)
DO UPDATE SET
    order_date_id   = EXCLUDED.order_date_id,
    ship_date_id    = EXCLUDED.ship_date_id,
    dim_customer_sk = EXCLUDED.dim_customer_sk,
    dim_product_sk  = EXCLUDED.dim_product_sk,
    geo_id          = EXCLUDED.geo_id,
    ship_mode       = EXCLUDED.ship_mode,
    sales_channel   = EXCLUDED.sales_channel,
    order_status    = EXCLUDED.order_status,
    quantity        = EXCLUDED.quantity,
    unit_price      = EXCLUDED.unit_price,
    unit_cost       = EXCLUDED.unit_cost,
    discount        = EXCLUDED.discount,
    total_revenue   = EXCLUDED.total_revenue,
    total_cost      = EXCLUDED.total_cost,
    profit          = EXCLUDED.profit,
    _loaded_at      = EXCLUDED._loaded_at;
