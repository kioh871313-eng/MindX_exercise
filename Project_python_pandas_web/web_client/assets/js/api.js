/**
 * api.js — Centralized fetch wrapper for the Flask API.
 * All functions return parsed JSON or throw on network/HTTP error.
 */

// Auto-detect API base URL:
// - When served by Flask (local or via ngrok): same origin, no port suffix needed
// - When opened directly as file (file://): fall back to localhost:5000
const BASE_URL = window.location.protocol === "file:"
    ? "http://127.0.0.1:5000"
    : window.location.origin;

// ---------------------------------------------------------------------------
// Internal helper
// ---------------------------------------------------------------------------
async function _get(path, params = {}) {
    // Keep "false" strings — only drop truly empty/null/undefined values
    const qs = new URLSearchParams(
        Object.entries(params).filter(([, v]) => v !== "" && v !== null && v !== undefined)
    ).toString();
    const url = qs ? `${BASE_URL}${path}?${qs}` : `${BASE_URL}${path}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`API error ${res.status}: ${path}`);
    return res.json();
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/** { min_date, max_date } — actual date range in the dataset */
async function getDateRange() {
    return _get("/metrics/date-range");
}

/** Total revenue, transactions, avg order, refund count & rate. */
async function getSummary(filters = {}) {
    return _get("/metrics/summary", filters);
}

/** { total_revenue: number } */
async function getTotalRevenue(filters = {}) {
    return _get("/metrics/revenue", filters);
}

/** { category_name: revenue, ... } */
async function getRevenueByCategory(filters = {}) {
    return _get("/metrics/category", filters);
}

/** { brand_name: revenue, ... } */
async function getRevenueByBrand(filters = {}) {
    return _get("/metrics/brand", filters);
}

/** { "YYYY-MM": revenue, ... } */
async function getMonthlyRevenue(filters = {}) {
    return _get("/metrics/monthly", filters);
}

/** { region_name: revenue, ... } */
async function getRevenueByRegion(filters = {}) {
    return _get("/metrics/region", filters);
}

/** { product_name: revenue, ... } — top N products */
async function getTopProducts(filters = {}, n = 10) {
    return _get("/metrics/top-products", { ...filters, n });
}

/** { segment_name: revenue, ... } */
async function getRevenueBySegment(filters = {}) {
    return _get("/metrics/segment", filters);
}

// ---------------------------------------------------------------------------
// Filters / table data
// ---------------------------------------------------------------------------

/**
 * Combined filter — primary data fetch for the table.
 * @param {Object} params  { category, brand, region, date_from, date_to,
 *                           include_refund, keyword, limit, offset }
 * @returns {{ total, limit, offset, data: Array }}
 */
async function fetchCombined(params = {}) {
    return _get("/filter/combined", params);
}

/**
 * Filter by category only (backward-compat).
 * @returns {{ total, limit, offset, data: Array }}
 */
async function filterByCategory(category, limit = 500, offset = 0) {
    return _get("/filter/category", { category, limit, offset });
}

/**
 * Keyword search (backward-compat).
 * @returns {{ total, limit, offset, data: Array }}
 */
async function searchKeyword(keyword, limit = 500, offset = 0) {
    return _get("/filter/search", { keyword, limit, offset });
}

/**
 * Distinct values for dropdowns: { categories, brands, regions, payments }
 */
async function getFilterOptions() {
    return _get("/filter/options");
}
