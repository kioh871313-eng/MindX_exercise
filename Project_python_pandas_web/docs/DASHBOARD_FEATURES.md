# Dashboard Features

**Pages:** `web_client/components/summary.html` · `web_client/components/transactions.html`  
**Tech:** Vanilla JS (ES6) · Highcharts · Flask REST API · CSS Grid

---

## Navigation

Two pages share a sticky header with tab navigation:

| Tab | File | Purpose |
|---|---|---|
| **Summary** | `summary.html` | KPI cards + 6 charts + filter panel |
| **Transactions** | `transactions.html` | Full transaction table + search + detail modal |

---

## Shared Filter Panel

Both pages include the same filter panel (Transactions page also has Search).
Filters apply immediately on change — no Apply button needed.

| Filter | Type | Page | Description |
|---|---|---|---|
| **Category** | Dropdown | Both | Product category: CPU, GPU, RAM, SSD, HDD, Monitor, Mouse, Keyboard, Case, Fan, Motherboard, Power Supply, VGA |
| **Brand** | Dropdown | Both | 13 brands: Intel, AMD, NVIDIA, Asus, MSI, Gigabyte, Corsair, G.Skill, Logitech, Razer, Samsung, Dell, HP |
| **Region** | Dropdown | Both | North America, Europe, Asia Pacific, Vietnam, Southeast Asia, Middle East |
| **Date From** | Date picker | Both | Start of range — default: 12 months before max date in dataset |
| **Date To** | Date picker | Both | End of range — default: max date in dataset |
| **Include Refunds** | Toggle switch | Both | When off, excludes Is_Refund = True rows |
| **Search** | Text input (debounce 400ms) | Transactions only | Keyword search across Product_Name, Brand, Category, Region |
| **Reset** | Button | Both | Restores all filters to defaults |

> Default dates are fetched from `GET /metrics/date-range` on page load, so they always reflect the actual dataset range — not the current system date.

---

## Page 1 — Summary

### Summary Cards

Four KPI cards at the top. All values re-fetch on every filter change.

| Card | Metric | Source |
|---|---|---|
| Total Revenue | Sum of Total_Amount_USD | `GET /metrics/summary` |
| Total Transactions | Row count after filters | `GET /metrics/summary` |
| Avg Order Value | Mean of Total_Amount_USD | `GET /metrics/summary` |
| Refunds | Count + refund rate % | `GET /metrics/summary` |

### Zone 1 — Top Performers (2 columns)

Two ranking charts side by side for direct comparison.

**Top Brands by Revenue**
- Type: Horizontal bar chart, top 10 brands
- API: `GET /metrics/brand`
- Use: Compare brand performance and market share

**Top 10 Products by Revenue**
- Type: Horizontal bar chart with value labels
- API: `GET /metrics/top-products?n=10`
- Use: Identify bestselling individual SKUs

### Zone 2 — Revenue Trend (full width)

**Monthly Revenue Trend**
- Type: Area chart
- API: `GET /metrics/monthly`
- Use: Spot seasonality, growth trends, and dips over the selected period
- X-axis auto-scales tick interval based on number of months shown

### Zone 3 — Revenue Breakdown (3 columns)

Three breakdown charts side by side.

**Revenue by Category**
- Type: Column chart, colour-coded per category
- API: `GET /metrics/category`
- Use: Compare hardware category contribution

**Revenue by Region**
- Type: Horizontal bar chart with value labels
- API: `GET /metrics/region`
- Use: Compare geographic revenue — easy to read differences vs pie chart

**Revenue by Customer Segment**
- Type: Column chart
- API: `GET /metrics/segment`
- Use: Compare sales across Retail, Wholesale, Online, Enterprise, Government

### Chart Subtitles

Every chart subtitle reflects the active filter state, e.g.:
`GPU · NVIDIA · Vietnam · 2024-01-01 → 2025-01-01`
When no filters are active, subtitle shows `All data`.

---

## Page 2 — Transactions

### Transaction Table

Paginated data grid — 100 rows per page.

| Column | Field | Notes |
|---|---|---|
| ID | Transaction_ID | Monospace font |
| Date | Transaction_Date | ISO string YYYY-MM-DD |
| Category | Product_Category | Colour badge |
| Brand | Brand | Title-cased |
| Product | Product_Name | Truncated with tooltip if long |
| Unit Price | Unit_Price_USD | Right-aligned, $0.00 format |
| Qty | Quantity | Right-aligned |
| Total | Total_Amount_USD | Right-aligned, bold |
| Region | Region | Title-cased |
| Payment | Payment_Method | Title-cased |
| Refund | Is_Refund | Green "No" / Red "Yes" badge |

**Pagination:** Prev / Next buttons · "Showing X–Y of Z records" info synced top and bottom.

### Transaction Detail Modal

Click any row to open a modal popup showing all 12 fields in a key-value grid.

Displayed fields: Transaction ID, Date, Product, Category, Brand, Unit Price, Quantity,
Total Amount, Customer Segment, Region, Payment Method, Is Refund.

Close: × button · click backdrop · Escape key.

---

## Loading State

Full-screen semi-transparent overlay with spinner during:
- Initial page load
- Any filter change
- Page navigation (table)

---

## Responsive Breakpoints

| Breakpoint | Behaviour |
|---|---|
| ≥ 1100px | 4-column KPI cards · 3-column zone 3 charts |
| < 1100px | 2-column KPI cards · 2-column zone 3 charts |
| < 900px | All chart grids → 1 column · compact header |
| < 600px | 2-column KPI cards · 1-column filter grid |
| < 400px | Full single-column layout |
