-- =============================================================
-- Gold — dim_date (Type 1, full rebuild)
-- Source : silver.clean_hardware_sales
-- Target : gold.dim_date
-- Purpose: Generate a complete date dimension from every unique
--          order_date and ship_date present in silver data.
--          Type 1 — no history needed, rebuilt daily.
--
-- Called by: dag_gold_build.py
-- =============================================================

-- Step 1: Collect all unique dates from both order_date and ship_date
WITH all_dates AS (
    SELECT order_date AS dt FROM silver.clean_hardware_sales WHERE order_date IS NOT NULL
    UNION
    SELECT ship_date  AS dt FROM silver.clean_hardware_sales WHERE ship_date  IS NOT NULL
)
INSERT INTO gold.dim_date (
    date_id,
    date,
    year,
    quarter,
    month,
    month_name,
    week,
    day_of_week,
    day_name,
    is_weekend
)
SELECT
    TO_CHAR(dt, 'YYYYMMDD')::INTEGER            AS date_id,
    dt                                           AS date,
    EXTRACT(YEAR    FROM dt)::SMALLINT           AS year,
    EXTRACT(QUARTER FROM dt)::SMALLINT           AS quarter,
    EXTRACT(MONTH   FROM dt)::SMALLINT           AS month,
    TO_CHAR(dt, 'Month')                         AS month_name,
    EXTRACT(WEEK    FROM dt)::SMALLINT           AS week,
    -- 0 = Sunday … 6 = Saturday  (PostgreSQL EXTRACT DOW convention)
    EXTRACT(DOW FROM dt)::SMALLINT               AS day_of_week,
    TO_CHAR(dt, 'Day')                           AS day_name,
    EXTRACT(DOW FROM dt) IN (0, 6)               AS is_weekend
FROM all_dates

ON CONFLICT (date_id)
DO UPDATE SET
    date        = EXCLUDED.date,
    year        = EXCLUDED.year,
    quarter     = EXCLUDED.quarter,
    month       = EXCLUDED.month,
    month_name  = EXCLUDED.month_name,
    week        = EXCLUDED.week,
    day_of_week = EXCLUDED.day_of_week,
    day_name    = EXCLUDED.day_name,
    is_weekend  = EXCLUDED.is_weekend;
