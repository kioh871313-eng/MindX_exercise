# Flow Architecture

Describes the data flow and component interactions across all layers of the project.

---

## System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        PROJECT LAYERS                               │
│                                                                     │
│  [1. Data Generation] → [2. ETL] → [3. Clean CSV]                  │
│                                          │                          │
│                                    [4. Flask API]                   │
│                                          │                          │
│                              [5. Browser / Dashboard]               │
│                          summary.html  ←→  transactions.html        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Layer 1 — Data Generation

**File:** `data_source/data_generator.py`

```
data_generator.py
      │
      │  Python random + datetime
      │  20,000 rows, 12 columns
      │  Intentionally injects dirty data
      ▼
hardware_sales_2019_2025_raw.csv
```

**What happens:**
- Generates 20,000 synthetic transactions with deliberate data quality issues:
  mixed date formats, prices with currency characters, negative quantities,
  messy boolean strings, inconsistent brand/category casing.
- Writes the result to `raw.csv` in the same folder.

---

## Layer 2 — ETL Pipeline

**Files:** `etl/clean_data.py` (orchestrator) + `etl/util.py` (helpers)

```
hardware_sales_2019_2025_raw.csv
      │
      │  clean_data.py reads CSV
      │
      ├── util.parse_date()         → normalise Transaction_Date (tries 4 formats)
      ├── util.normalise_text()     → lowercase + strip all text columns
      ├── util.parse_price()        → strip currency characters → float
      ├── util.parse_quantity()     → strip "pcs" → int, discard negatives
      ├── util.parse_bool()         → normalise Is_Refund → True / False / None
      │   Recalculate: Total = Unit_Price × Quantity
      ├── util.validate_dataframe() → check schema, missing columns, negatives
      └── util.report_nulls()       → print null rate per column
      │
      ▼
hardware_sales_2019_2025_clean.csv
```

**Note:** The ETL runs manually. After each run, **restart the Flask API**
so it reloads the updated clean CSV into memory.

---

## Layer 3 — Flask REST API

**Files:** `api/app.py`, `api/routes/metrics.py`, `api/routes/filters.py`

```
api/app.py  (Flask application startup)
      │
      ├── Create Flask app
      ├── Attach _SafeJSONProvider  → serialise float NaN as null
      ├── CORS(app)                 → allow requests from file:// origins
      ├── Register metrics_bp       → prefix /metrics
      └── Register filters_bp       → prefix /filter
```

### How each Blueprint works

```
metrics.py / filters.py
      │
      │  Module load (once at Flask startup)
      ├── pd.read_csv(clean.csv)
      ├── Parse Transaction_Date
      ├── Normalise text columns
      └── _df  (global DataFrame held in memory)
      │
      │  On each HTTP request
      ├── _get_filters()       reads query params from request
      ├── _apply_filters(_df)  filters the DataFrame
      ├── Aggregate / sort / paginate
      └── jsonify(result)      returns HTTP JSON response
```

### Request / response flow

```
Browser                    Flask                        Pandas
  │                          │                            │
  │  GET /metrics/summary    │                            │
  │ ─────────────────────>   │  _apply_filters(_df)  ─>  │
  │                          │  sum, mean, count     <─  │
  │  { total_revenue, ... }  │                            │
  │ <─────────────────────   │                            │
  │                          │                            │
  │  GET /filter/combined    │                            │
  │  ?category=gpu&limit=100 │                            │
  │ ─────────────────────>   │  _apply_filters()     ─>  │
  │                          │  _paginate()          <─  │
  │                          │  _serialize()              │
  │  { total, data: [...] }  │                            │
  │ <─────────────────────   │                            │
```

---

## Layer 4 — Frontend

### JavaScript file responsibilities

```
api.js          → All fetch calls. The only file that talks to the API.
ui-shared.js    → Global filter state, DOM helpers, initFilters(), date defaults.
charts.js       → Highcharts renderers for all 6 charts. Uses functions from api.js.
summary.js      → Initialises summary.html: loads cards + refreshAllCharts().
transactions.js → Initialises transactions.html: table render, modal, pagination.
```

### Script load order in HTML

```html
<!-- summary.html -->
<script src="../assets/js/api.js"></script>        ← must load first
<script src="../assets/js/charts.js"></script>     ← depends on api.js
<script src="../assets/js/ui-shared.js"></script>  ← depends on api.js
<script src="../assets/js/summary.js"></script>    ← depends on all three above

<!-- transactions.html -->
<script src="../assets/js/api.js"></script>
<script src="../assets/js/ui-shared.js"></script>
<script src="../assets/js/transactions.js"></script>
```

### Flow — user opens Summary page

```
DOMContentLoaded
      │
      ▼
summary.js: _init()
      │
      ├── initFilters(refreshSummaryPage)   [ui-shared.js]
      │       ├── GET /metrics/date-range   [api.js]
      │       ├── Set date inputs (max - 12 months → max)
      │       └── Bind change events on all filter inputs
      │
      └── refreshSummaryPage()
              │
              ├── GET /metrics/summary  → updateSummaryCards()
              │                              updates 4 DOM stat values
              │
              └── refreshAllCharts(filters)   [charts.js]
                      ├── GET /metrics/brand        → Highcharts "brandChart"
                      ├── GET /metrics/top-products → Highcharts "topProductsChart"
                      ├── GET /metrics/monthly      → Highcharts "monthlyChart"
                      ├── GET /metrics/category     → Highcharts "categoryChart"
                      ├── GET /metrics/region       → Highcharts "regionChart"
                      └── GET /metrics/segment      → Highcharts "segmentChart"
```

### Flow — user opens Transactions page

```
DOMContentLoaded
      │
      ▼
transactions.js
      │
      ├── bindPagination()   → attach Prev / Next click handlers
      ├── bindModal()        → attach close handlers to modal
      │
      └── initFilters(loadTablePage)   [ui-shared.js]
              │
              ├── GET /metrics/date-range → set default dates
              └── Bind all filter events → trigger loadTablePage(0)
                      │
                      └── loadTablePage(0)
                              │
                              ├── GET /filter/combined?...&limit=100&offset=0
                              └── updateTable(response)
                                      ├── Render <tr> rows into <tbody>
                                      ├── Attach click → openModal(row)
                                      └── updatePaginationUI()
```

### Flow — user changes a filter

```
User changes dropdown / date / toggle / search input
      │
      ▼
Event listener (change / input)   [ui-shared.js: initFilters()]
      │
      ├── readFilters()   → update currentFilters object from DOM
      └── onChange()      → refreshSummaryPage()  OR  loadTablePage(0)

              Summary page:
              ├── GET /metrics/summary    → update 4 stat cards
              └── refreshAllCharts()     → 6 parallel API calls (Promise.all)
                                           → Highcharts re-renders all charts

              Transactions page:
              └── GET /filter/combined   → updateTable()
                                         → reset to page 0
```

### Flow — user clicks a table row

```
User clicks <tr>
      │
      ▼
tr.addEventListener("click", () => openModal(row))
      │
      ▼
openModal(row)   [transactions.js]
      ├── Build HTML from 12 fields of the row object (already in memory, no API call)
      ├── Inject into #modal-body
      └── modal.classList.remove("hidden")
```

### Flow — pagination

```
User clicks Next
      │
      ▼
btn-next click → loadTablePage(currentPage + 1)
      │
      └── GET /filter/combined?...&offset=(page * 100)
              └── updateTable(response)
                      └── updatePaginationUI()
                              ├── "Showing X–Y of Z records"
                              ├── prev.disabled = (page === 0)
                              └── next.disabled = (end >= total)
```

---

## File Dependency Map

```
api.js
  ↑
  ├── charts.js          (calls getRevenueByCategory, getMonthlyRevenue, ...)
  ├── ui-shared.js       (calls getDateRange)
  └── transactions.js    (calls fetchCombined)

ui-shared.js
  ↑
  ├── summary.js         (calls initFilters, readFilters, filterLabel, fmtMoney, ...)
  └── transactions.js    (calls initFilters, readFilters, fmtMoney, fmtNumber, ...)

charts.js
  ↑
  └── summary.js         (calls refreshAllCharts)
```

---

## End-to-End Data Flow (CSV → Chart)

```
hardware_sales_2019_2025_clean.csv
      │  pd.read_csv()  —  once at Flask startup
      ▼
_df  (pandas DataFrame held in process memory)
      │  _apply_filters()  —  on every request
      ▼
Filtered DataFrame
      │  .groupby().sum()  /  .head(n)  /  .iloc[offset : offset + limit]
      ▼
Python dict / list
      │  jsonify() + _SafeJSONProvider  (NaN → null)
      ▼
JSON HTTP Response
      │  fetch() in api.js
      ▼
JavaScript object
      │  Highcharts.chart()  /  DOM manipulation
      ▼
Chart render / Table rows in the browser
```

---

## Key Technical Notes

| Issue | Solution |
|---|---|
| Flask serialises `float NaN` as literal `NaN`, which is invalid JSON | `_SafeJSONProvider` in `app.py` walks the entire object tree and replaces NaN / Infinity with `None` before serialisation |
| API loads data once at startup and does not auto-reload | Restart the Flask process after every `etl/clean_data.py` run |
| Browser cannot call the API from `file://` due to CORS | `flask_cors.CORS(app)` enabled for all origins |
| Default date range should reflect actual dataset, not today's system date | `GET /metrics/date-range` returns the true max date from the dataset; JS uses that value instead of `new Date()` |
| Search should not fire an API call on every keystroke | 400 ms debounce in `ui-shared.js` |
| Transaction detail modal needs no extra API call | Row data is already in memory from the table fetch; `openModal(row)` simply renders it |
