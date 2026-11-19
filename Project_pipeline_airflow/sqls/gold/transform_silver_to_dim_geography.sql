-- =============================================================
-- Gold — dim_geography (Type 1, upsert)
-- Source : silver.clean_hardware_sales
-- Target : gold.dim_geography
-- Purpose: Build the geography dimension from unique combinations
--          of country / region / state / city / postal_code.
--          Type 1 — no SCD2 needed for geography.
--
-- Called by: dag_gold_build.py
-- =============================================================

INSERT INTO gold.dim_geography (
    country,
    region,
    state,
    city,
    postal_code
)
SELECT DISTINCT ON (country, state, city, postal_code)
    country,
    region,
    state,
    city,
    postal_code
FROM silver.clean_hardware_sales
WHERE
    country IS NOT NULL
    OR city IS NOT NULL
ORDER BY country, state, city, postal_code

ON CONFLICT (country, state, city, postal_code)
DO UPDATE SET
    region = EXCLUDED.region;
