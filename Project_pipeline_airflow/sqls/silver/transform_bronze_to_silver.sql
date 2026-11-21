-- =============================================================
-- Silver Layer Transformation
-- Source : bronze.raw_hardware_sales
-- Target : silver.clean_hardware_sales
-- Purpose: Clean, type-cast, deduplicate, and standardise raw
--          bronze data. All transformation logic lives here.
--
-- Idempotency: INSERT … ON CONFLICT (order_id) DO UPDATE
--   Safe to re-run any number of times per day.
--
-- Called by: dag_pipeline.py via execute_sql_file()
-- Snapshot model: processes only the latest snapshot per source
--   file (_ingested_at = MAX per _source_file). No runtime
--   parameters required.
-- =============================================================

INSERT INTO silver.clean_hardware_sales (
    order_id,
    order_date,
    ship_date,
    ship_mode,
    order_status,
    sales_channel,
    customer_id,
    customer_name,
    customer_segment,
    country,
    city,
    state,
    postal_code,
    region,
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    unit_price,
    unit_cost,
    quantity,
    discount,
    total_revenue,
    total_cost,
    profit,
    _source_file,
    _bronze_id,
    _loaded_at
)

SELECT
    -- -------------------------------------------------------
    -- Keys: trim whitespace, skip fully-NULL rows
    -- -------------------------------------------------------
    TRIM(order_id)                                          AS order_id,

    -- -------------------------------------------------------
    -- Dates: handle multiple common formats gracefully.
    -- Any format PostgreSQL cannot parse becomes NULL.
    --
    -- Format detection logic for XX/XX/XXXX ambiguity:
    --   If the first segment > 12, it cannot be a month
    --   → must be DD/MM/YYYY (e.g. "17/11/2022")
    --   Otherwise treat as MM/DD/YYYY (project default from generator)
    -- -------------------------------------------------------
    CASE
        WHEN TRIM(order_date) ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TO_DATE(TRIM(order_date), 'YYYY-MM-DD')
        WHEN TRIM(order_date) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$'
            THEN TO_DATE(TRIM(order_date), 'DD-Mon-YYYY')
        WHEN TRIM(order_date) ~ '^\d{2}/\d{2}/\d{4}$'
            THEN CASE
                WHEN SPLIT_PART(TRIM(order_date), '/', 1)::INTEGER > 12
                    THEN TO_DATE(TRIM(order_date), 'DD/MM/YYYY')
                ELSE
                    TO_DATE(TRIM(order_date), 'MM/DD/YYYY')
            END
        ELSE NULL
    END                                                     AS order_date,

    CASE
        WHEN TRIM(ship_date) ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TO_DATE(TRIM(ship_date), 'YYYY-MM-DD')
        WHEN TRIM(ship_date) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$'
            THEN TO_DATE(TRIM(ship_date), 'DD-Mon-YYYY')
        WHEN TRIM(ship_date) ~ '^\d{2}/\d{2}/\d{4}$'
            THEN CASE
                WHEN SPLIT_PART(TRIM(ship_date), '/', 1)::INTEGER > 12
                    THEN TO_DATE(TRIM(ship_date), 'DD/MM/YYYY')
                ELSE
                    TO_DATE(TRIM(ship_date), 'MM/DD/YYYY')
            END
        ELSE NULL
    END                                                     AS ship_date,

    -- -------------------------------------------------------
    -- Categorical strings: trim + normalise casing
    -- -------------------------------------------------------
    INITCAP(TRIM(ship_mode))                                AS ship_mode,
    INITCAP(TRIM(order_status))                             AS order_status,
    INITCAP(TRIM(sales_channel))                            AS sales_channel,

    TRIM(customer_id)                                       AS customer_id,
    INITCAP(TRIM(customer_name))                            AS customer_name,
    INITCAP(TRIM(customer_segment))                         AS customer_segment,

    INITCAP(TRIM(country))                                  AS country,
    INITCAP(TRIM(city))                                     AS city,
    INITCAP(TRIM(state))                                    AS state,
    TRIM(postal_code)                                       AS postal_code,
    -- NULL or empty region defaults to 'Unknown'
    COALESCE(NULLIF(TRIM(region), ''), 'Unknown')           AS region,

    TRIM(product_id)                                        AS product_id,
    INITCAP(TRIM(product_name))                             AS product_name,
    INITCAP(TRIM(category))                                 AS category,
    INITCAP(TRIM(sub_category))                             AS sub_category,
    INITCAP(TRIM(brand))                                    AS brand,

    -- -------------------------------------------------------
    -- Numerics: strip commas (e.g. "1,250.00"), cast to NUMERIC.
    -- Negative values or parse failures become NULL.
    -- -------------------------------------------------------
    CASE
        WHEN NULLIF(TRIM(REPLACE(unit_price, ',', '')), '') ~ '^-?[0-9]+(\.[0-9]+)?$'
         AND NULLIF(TRIM(REPLACE(unit_price, ',', '')), '')::NUMERIC >= 0
            THEN NULLIF(TRIM(REPLACE(unit_price, ',', '')), '')::NUMERIC(12,2)
        ELSE NULL
    END                                                     AS unit_price,

    CASE
        WHEN NULLIF(TRIM(REPLACE(unit_cost, ',', '')), '') ~ '^-?[0-9]+(\.[0-9]+)?$'
         AND NULLIF(TRIM(REPLACE(unit_cost, ',', '')), '')::NUMERIC >= 0
            THEN NULLIF(TRIM(REPLACE(unit_cost, ',', '')), '')::NUMERIC(12,2)
        ELSE NULL
    END                                                     AS unit_cost,

    CASE
        WHEN NULLIF(TRIM(REPLACE(quantity, ',', '')), '') ~ '^-?[0-9]+$'
         AND NULLIF(TRIM(REPLACE(quantity, ',', '')), '')::INTEGER >= 0
            THEN NULLIF(TRIM(REPLACE(quantity, ',', '')), '')::INTEGER
        ELSE NULL
    END                                                     AS quantity,

    -- Discount: NULL or empty → 0.0
    COALESCE(
        CASE
            WHEN NULLIF(TRIM(REPLACE(discount, ',', '')), '') ~ '^-?[0-9]+(\.[0-9]+)?$'
             AND NULLIF(TRIM(REPLACE(discount, ',', '')), '')::NUMERIC BETWEEN 0 AND 1
                THEN NULLIF(TRIM(REPLACE(discount, ',', '')), '')::NUMERIC(5,4)
            ELSE NULL
        END,
        0.0
    )                                                       AS discount,

    -- -------------------------------------------------------
    -- Recalculate measures from source values for consistency.
    -- This deliberately overrides any calculation errors in source.
    -- -------------------------------------------------------
    ROUND(
        CASE
            WHEN NULLIF(TRIM(REPLACE(unit_price, ',', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
             AND NULLIF(TRIM(REPLACE(quantity,   ',', '')), '') ~ '^[0-9]+$'
                THEN NULLIF(TRIM(REPLACE(unit_price, ',', '')), '')::NUMERIC
                   * NULLIF(TRIM(REPLACE(quantity,   ',', '')), '')::NUMERIC
                   * (1 - COALESCE(
                            CASE WHEN NULLIF(TRIM(REPLACE(discount, ',','')), '') ~ '^[0-9]+(\.[0-9]+)?$'
                                 THEN NULLIF(TRIM(REPLACE(discount,',','')), '')::NUMERIC
                                 ELSE NULL END,
                            0))
            ELSE NULL
        END, 2)                                             AS total_revenue,

    ROUND(
        CASE
            WHEN NULLIF(TRIM(REPLACE(unit_cost, ',', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
             AND NULLIF(TRIM(REPLACE(quantity,  ',', '')), '') ~ '^[0-9]+$'
                THEN NULLIF(TRIM(REPLACE(unit_cost, ',', '')), '')::NUMERIC
                   * NULLIF(TRIM(REPLACE(quantity,  ',', '')), '')::NUMERIC
            ELSE NULL
        END, 2)                                             AS total_cost,

    -- profit is always derived here — never trusted from source
    ROUND(
        CASE
            WHEN NULLIF(TRIM(REPLACE(unit_price, ',', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
             AND NULLIF(TRIM(REPLACE(unit_cost,  ',', '')), '') ~ '^[0-9]+(\.[0-9]+)?$'
             AND NULLIF(TRIM(REPLACE(quantity,   ',', '')), '') ~ '^[0-9]+$'
                THEN (
                    NULLIF(TRIM(REPLACE(unit_price, ',', '')), '')::NUMERIC
                  * NULLIF(TRIM(REPLACE(quantity,   ',', '')), '')::NUMERIC
                  * (1 - COALESCE(
                            CASE WHEN NULLIF(TRIM(REPLACE(discount, ',','')), '') ~ '^[0-9]+(\.[0-9]+)?$'
                                 THEN NULLIF(TRIM(REPLACE(discount,',','')), '')::NUMERIC
                                 ELSE NULL END,
                            0))
                ) - (
                    NULLIF(TRIM(REPLACE(unit_cost, ',', '')), '')::NUMERIC
                  * NULLIF(TRIM(REPLACE(quantity,  ',', '')), '')::NUMERIC
                )
            ELSE NULL
        END, 2)                                             AS profit,

    -- -------------------------------------------------------
    -- Lineage / metadata
    -- -------------------------------------------------------
    _source_file,
    id                                                      AS _bronze_id,
    NOW()                                                   AS _loaded_at

FROM bronze.raw_hardware_sales

WHERE
    -- CDC incremental filter — only rows from the most recent load of this file
    _ingested_at = (
        SELECT MAX(b2._ingested_at)
        FROM bronze.raw_hardware_sales b2
        WHERE b2._source_file = bronze.raw_hardware_sales._source_file
    )
    -- Skip rows with NULL key columns (they cannot be merged downstream)
    AND TRIM(COALESCE(order_id,   '')) <> ''
    AND TRIM(COALESCE(customer_id,'')) <> ''
    AND TRIM(COALESCE(product_id, '')) <> ''

ON CONFLICT (order_id)
DO UPDATE SET
    order_date       = EXCLUDED.order_date,
    ship_date        = EXCLUDED.ship_date,
    ship_mode        = EXCLUDED.ship_mode,
    order_status     = EXCLUDED.order_status,
    sales_channel    = EXCLUDED.sales_channel,
    customer_id      = EXCLUDED.customer_id,
    customer_name    = EXCLUDED.customer_name,
    customer_segment = EXCLUDED.customer_segment,
    country          = EXCLUDED.country,
    city             = EXCLUDED.city,
    state            = EXCLUDED.state,
    postal_code      = EXCLUDED.postal_code,
    region           = EXCLUDED.region,
    product_id       = EXCLUDED.product_id,
    product_name     = EXCLUDED.product_name,
    category         = EXCLUDED.category,
    sub_category     = EXCLUDED.sub_category,
    brand            = EXCLUDED.brand,
    unit_price       = EXCLUDED.unit_price,
    unit_cost        = EXCLUDED.unit_cost,
    quantity         = EXCLUDED.quantity,
    discount         = EXCLUDED.discount,
    total_revenue    = EXCLUDED.total_revenue,
    total_cost       = EXCLUDED.total_cost,
    profit           = EXCLUDED.profit,
    _source_file     = EXCLUDED._source_file,
    _bronze_id       = EXCLUDED._bronze_id,
    _loaded_at       = EXCLUDED._loaded_at;
