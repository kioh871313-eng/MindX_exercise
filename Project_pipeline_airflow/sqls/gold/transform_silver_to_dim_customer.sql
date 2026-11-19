-- =============================================================
-- Gold — dim_customer (SCD Type 2)
-- Source : silver.clean_hardware_sales
-- Target : gold.dim_customer
-- Purpose: Upsert customer dimension with SCD2 logic.
--
-- SCD2 strategy (same pattern as dim_product):
--   1. Close active records where a tracked attribute changed.
--   2. Insert a new active version for each changed / new customer.
--
-- Tracked columns:
--   customer_name, customer_segment, country, city, region
--
-- Called by: dag_gold_build.py
-- =============================================================

-- -------------------------------------------------------
-- Step 1: Close stale active records.
-- -------------------------------------------------------
UPDATE gold.dim_customer AS dim
SET
    valid_to   = CURRENT_DATE,
    is_current = FALSE
FROM (
    SELECT DISTINCT ON (customer_id)
        customer_id,
        customer_name,
        customer_segment,
        country,
        city,
        region
    FROM silver.clean_hardware_sales
    WHERE customer_id IS NOT NULL
    ORDER BY customer_id
) AS latest
WHERE
    dim.customer_id  = latest.customer_id
    AND dim.is_current = TRUE
    AND (
        dim.customer_name    IS DISTINCT FROM latest.customer_name    OR
        dim.customer_segment IS DISTINCT FROM latest.customer_segment OR
        dim.country          IS DISTINCT FROM latest.country          OR
        dim.city             IS DISTINCT FROM latest.city             OR
        dim.region           IS DISTINCT FROM latest.region
    );


-- -------------------------------------------------------
-- Step 2: Insert new active version for new / changed customers.
-- -------------------------------------------------------
INSERT INTO gold.dim_customer (
    customer_id,
    customer_name,
    customer_segment,
    country,
    city,
    region,
    valid_from,
    valid_to,
    is_current
)
SELECT DISTINCT ON (s.customer_id)
    s.customer_id,
    s.customer_name,
    s.customer_segment,
    s.country,
    s.city,
    s.region,
    CURRENT_DATE    AS valid_from,
    NULL            AS valid_to,
    TRUE            AS is_current
FROM silver.clean_hardware_sales s
WHERE
    s.customer_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM gold.dim_customer d
        WHERE d.customer_id = s.customer_id
          AND d.is_current  = TRUE
    )
ORDER BY s.customer_id;
