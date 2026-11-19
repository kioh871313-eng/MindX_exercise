-- =============================================================
-- Gold Layer — Sample Analytical Queries
-- Purpose : Reference queries for reporting on the star schema.
-- =============================================================

-- -------------------------------------------------------------
-- 1. Total revenue, cost, and profit by year and quarter
-- -------------------------------------------------------------
SELECT
    d.year,
    d.quarter,
    SUM(f.total_revenue)    AS total_revenue,
    SUM(f.total_cost)       AS total_cost,
    SUM(f.profit)           AS total_profit,
    ROUND(SUM(f.profit) / NULLIF(SUM(f.total_revenue), 0) * 100, 2) AS profit_margin_pct
FROM gold.fact_sales f
JOIN gold.dim_date d  ON f.order_date_id = d.date_id
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;


-- -------------------------------------------------------------
-- 2. Top 10 products by revenue (current version only)
-- -------------------------------------------------------------
SELECT
    p.product_name,
    p.category,
    p.sub_category,
    SUM(f.total_revenue)    AS total_revenue,
    SUM(f.quantity)         AS units_sold
FROM gold.fact_sales f
JOIN gold.dim_product p ON f.dim_product_sk = p.dim_product_sk
WHERE p.is_current = TRUE
GROUP BY p.product_name, p.category, p.sub_category
ORDER BY total_revenue DESC
LIMIT 10;


-- -------------------------------------------------------------
-- 3. Sales performance by region
-- -------------------------------------------------------------
SELECT
    g.region,
    g.country,
    COUNT(DISTINCT f.order_id)  AS total_orders,
    SUM(f.total_revenue)        AS total_revenue,
    SUM(f.profit)               AS total_profit
FROM gold.fact_sales f
JOIN gold.dim_geography g ON f.geo_id = g.geo_id
GROUP BY g.region, g.country
ORDER BY total_revenue DESC;


-- -------------------------------------------------------------
-- 4. Monthly revenue trend
-- -------------------------------------------------------------
SELECT
    d.year,
    d.month,
    d.month_name,
    SUM(f.total_revenue)    AS monthly_revenue,
    SUM(f.profit)           AS monthly_profit,
    COUNT(DISTINCT f.order_id) AS order_count
FROM gold.fact_sales f
JOIN gold.dim_date d ON f.order_date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- -------------------------------------------------------------
-- 5. Customer segment analysis
-- -------------------------------------------------------------
SELECT
    c.customer_segment,
    COUNT(DISTINCT c.customer_id)   AS customer_count,
    COUNT(DISTINCT f.order_id)      AS total_orders,
    SUM(f.total_revenue)            AS total_revenue,
    AVG(f.total_revenue)            AS avg_order_value
FROM gold.fact_sales f
JOIN gold.dim_customer c ON f.dim_customer_sk = c.dim_customer_sk
WHERE c.is_current = TRUE
GROUP BY c.customer_segment
ORDER BY total_revenue DESC;


-- -------------------------------------------------------------
-- 6. SCD2 — View product price history
-- -------------------------------------------------------------
SELECT
    product_id,
    product_name,
    unit_price,
    unit_cost,
    valid_from,
    valid_to,
    is_current
FROM gold.dim_product
WHERE product_id = :product_id   -- replace :product_id with actual value
ORDER BY valid_from;


-- -------------------------------------------------------------
-- 7. Pipeline execution log — last 7 days
-- -------------------------------------------------------------
SELECT
    dag_id,
    task_id,
    layer,
    status,
    rows_processed,
    error_message,
    logged_at
FROM logs.pipeline_log
WHERE logged_at >= NOW() - INTERVAL '7 days'
ORDER BY logged_at DESC;


-- -------------------------------------------------------------
-- 8. Profitability by category and sales channel
-- -------------------------------------------------------------
SELECT
    p.category,
    f.sales_channel,
    SUM(f.total_revenue)    AS revenue,
    SUM(f.profit)           AS profit,
    ROUND(SUM(f.profit) / NULLIF(SUM(f.total_revenue), 0) * 100, 2) AS margin_pct
FROM gold.fact_sales f
JOIN gold.dim_product p ON f.dim_product_sk = p.dim_product_sk
WHERE p.is_current = TRUE
GROUP BY p.category, f.sales_channel
ORDER BY revenue DESC;
