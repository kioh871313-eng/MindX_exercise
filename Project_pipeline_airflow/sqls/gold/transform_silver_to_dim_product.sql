-- =============================================================
-- Gold — dim_product (SCD Type 2)
-- Source : silver.clean_hardware_sales
-- Target : gold.dim_product
-- Purpose: Upsert product dimension with SCD2 logic.
--
-- SCD2 strategy (pure SQL):
--   1. Close any currently-active record where a tracked attribute
--      has changed (set valid_to = TODAY, is_current = FALSE).
--   2. Insert a new active version for each changed / new product.
--   3. Products with no attribute change are left untouched.
--
-- Tracked columns (changes trigger a new SCD2 version):
--   product_name, category, sub_category, brand, unit_cost, unit_price
--
-- Called by: dag_gold_build.py
-- =============================================================

-- -------------------------------------------------------
-- Step 1: Close stale active records where any tracked
--         attribute differs from the latest silver snapshot.
-- -------------------------------------------------------
UPDATE gold.dim_product AS dim
SET
    valid_to   = CURRENT_DATE,
    is_current = FALSE
FROM (
    SELECT DISTINCT ON (product_id)
        product_id,
        product_name,
        category,
        sub_category,
        brand,
        unit_cost,
        unit_price
    FROM silver.clean_hardware_sales
    WHERE product_id IS NOT NULL
    ORDER BY product_id
) AS latest
WHERE
    dim.product_id  = latest.product_id
    AND dim.is_current = TRUE
    AND (
        dim.product_name  IS DISTINCT FROM latest.product_name  OR
        dim.category      IS DISTINCT FROM latest.category      OR
        dim.sub_category  IS DISTINCT FROM latest.sub_category  OR
        dim.brand         IS DISTINCT FROM latest.brand         OR
        dim.unit_cost     IS DISTINCT FROM latest.unit_cost     OR
        dim.unit_price    IS DISTINCT FROM latest.unit_price
    );


-- -------------------------------------------------------
-- Step 2: Insert new active version for every product that
--         is either brand-new or just had its record closed.
-- -------------------------------------------------------
INSERT INTO gold.dim_product (
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    unit_cost,
    unit_price,
    valid_from,
    valid_to,
    is_current
)
SELECT DISTINCT ON (s.product_id)
    s.product_id,
    s.product_name,
    s.category,
    s.sub_category,
    s.brand,
    s.unit_cost,
    s.unit_price,
    CURRENT_DATE    AS valid_from,
    NULL            AS valid_to,
    TRUE            AS is_current
FROM silver.clean_hardware_sales s
WHERE
    s.product_id IS NOT NULL
    -- Only insert where there is no current active record
    -- (covers both brand-new products and those just closed above)
    AND NOT EXISTS (
        SELECT 1
        FROM gold.dim_product d
        WHERE d.product_id  = s.product_id
          AND d.is_current  = TRUE
    )
ORDER BY s.product_id;
