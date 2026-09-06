/**
 * ui-shared.js
 * Shared filter state, DOM helpers, and initialisation logic.
 * Loaded on both summary.html and transactions.html before page-specific JS.
 */

// ---------------------------------------------------------------------------
// Globals (written here, read by summary.js / transactions.js)
// ---------------------------------------------------------------------------
let currentFilters = {
    category:       "",
    brand:          "",
    region:         "",
    date_from:      "",
    date_to:        "",
    include_refund: "true",
    keyword:        "",
};

// Set once after /metrics/date-range resolves
let DB_MAX_DATE = "";
let DB_MIN_DATE = "";

// ---------------------------------------------------------------------------
// DOM helpers (lazy refs — safe to call before DOMContentLoaded)
// ---------------------------------------------------------------------------
const el = id => document.getElementById(id);

// ---------------------------------------------------------------------------
// Format helpers (shared across pages)
// ---------------------------------------------------------------------------
function fmtMoney(v) {
    if (v == null || isNaN(v)) return "—";
    if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`;
    if (v >= 1_000)     return `$${(v / 1_000).toFixed(1)}K`;
    return `$${Number(v).toFixed(2)}`;
}

function fmtNumber(v) {
    if (v == null) return "—";
    return Number(v).toLocaleString();
}

function fmtDate(v) {
    if (!v) return "—";
    return v;
}

// ---------------------------------------------------------------------------
// Loading overlay
// ---------------------------------------------------------------------------
function showLoading() {
    el("loading-overlay")?.classList.remove("hidden");
}
function hideLoading() {
    el("loading-overlay")?.classList.add("hidden");
}

// ---------------------------------------------------------------------------
// Debounce
// ---------------------------------------------------------------------------
function debounce(fn, ms) {
    let t;
    return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}

// ---------------------------------------------------------------------------
// Subtract 12 months from a YYYY-MM-DD string
// ---------------------------------------------------------------------------
function _minus12Months(isoDate) {
    const d = new Date(isoDate);
    d.setFullYear(d.getFullYear() - 1);
    return d.toISOString().slice(0, 10);
}

// ---------------------------------------------------------------------------
// Read all filter inputs into currentFilters
// ---------------------------------------------------------------------------
function readFilters() {
    currentFilters = {
        category:       el("filter-category")?.value  || "",
        brand:          el("filter-brand")?.value     || "",
        region:         el("filter-region")?.value    || "",
        date_from:      el("filter-date-from")?.value || "",
        date_to:        el("filter-date-to")?.value   || "",
        include_refund: el("filter-refund")?.checked ? "true" : "false",
        keyword:        el("filter-search")?.value?.trim() || "",
    };
    return currentFilters;
}

// ---------------------------------------------------------------------------
// Set filter inputs from currentFilters (used by reset)
// ---------------------------------------------------------------------------
function applyFiltersToDom(f) {
    const set = (id, val) => { const e = el(id); if (e) e.value = val; };
    set("filter-category", f.category);
    set("filter-brand",    f.brand);
    set("filter-region",   f.region);
    set("filter-date-from", f.date_from);
    set("filter-date-to",   f.date_to);
    const refund = el("filter-refund");
    if (refund) refund.checked = f.include_refund !== "false";
    const search = el("filter-search");
    if (search) search.value = f.keyword || "";
}

// ---------------------------------------------------------------------------
// Build a human-readable filter label (for chart subtitles)
// e.g. "CPU · Intel · Asia · 2024-01-01 → 2024-12-31"
// ---------------------------------------------------------------------------
function filterLabel() {
    const parts = [];
    if (currentFilters.category) parts.push(currentFilters.category.toUpperCase());
    if (currentFilters.brand)    parts.push(_titleCase(currentFilters.brand));
    if (currentFilters.region)   parts.push(_titleCase(currentFilters.region));
    if (currentFilters.date_from || currentFilters.date_to) {
        const from = currentFilters.date_from || "…";
        const to   = currentFilters.date_to   || "…";
        parts.push(`${from} → ${to}`);
    }
    return parts.length ? parts.join(" · ") : "All data";
}

function _titleCase(s) {
    return s.replace(/\b\w/g, c => c.toUpperCase());
}

// ---------------------------------------------------------------------------
// Initialise shared filter panel:
//   1. Fetch date range from API
//   2. Set default dates (max - 12m → max)
//   3. Bind all filter change events → onChange callback
//   4. Bind reset button
// ---------------------------------------------------------------------------
async function initFilters(onChange) {
    // 1. Get date range from DB
    try {
        const range = await getDateRange();
        DB_MAX_DATE = range.max_date || new Date().toISOString().slice(0, 10);
        DB_MIN_DATE = range.min_date || "";
    } catch {
        DB_MAX_DATE = new Date().toISOString().slice(0, 10);
    }

    // 2. Set default dates: last 12 months relative to DB max date
    const defaultDateTo   = DB_MAX_DATE;
    const defaultDateFrom = _minus12Months(DB_MAX_DATE);

    const dateFromEl = el("filter-date-from");
    const dateToEl   = el("filter-date-to");
    const refundEl   = el("filter-refund");

    if (dateFromEl) dateFromEl.value = defaultDateFrom;
    if (dateToEl)   dateToEl.value   = defaultDateTo;
    if (refundEl)   refundEl.checked = true;

    // Store in currentFilters
    currentFilters.date_from = defaultDateFrom;
    currentFilters.date_to   = defaultDateTo;

    // 3. Bind filter change events
    ["filter-category", "filter-brand", "filter-region",
     "filter-date-from", "filter-date-to", "filter-refund"].forEach(id => {
        el(id)?.addEventListener("change", () => { readFilters(); onChange(); });
    });

    // Search only exists on transactions page
    el("filter-search")?.addEventListener("input", debounce(() => {
        readFilters(); onChange();
    }, 400));

    // 4. Reset button
    el("btn-reset")?.addEventListener("click", () => {
        applyFiltersToDom({
            category: "", brand: "", region: "",
            date_from: defaultDateFrom, date_to: defaultDateTo,
            include_refund: "true", keyword: "",
        });
        readFilters();
        onChange();
    });
}
