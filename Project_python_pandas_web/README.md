# Hardware Sales Dashboard (2019–2025)

A full-stack data analytics project covering synthetic data generation, ETL cleaning,
a Flask REST API, and a two-page interactive web dashboard — built around a 20,000-row
computer hardware sales dataset.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data generation | Python (random, datetime) |
| ETL / data cleaning | Python, Pandas, NumPy |
| REST API | Flask 3.1.1, Flask-CORS 5.0.1 |
| Frontend | HTML5, CSS3, JavaScript (ES6, Vanilla) |
| Charts | Highcharts (CDN) |
| Version control | Git / GitHub |

---

## Dataset Overview

`hardware_sales_2019_2025_raw.csv` — 20,000 synthetic rows with intentionally dirty data.

**Columns:** Transaction_ID · Transaction_Date · Product_Category · Product_Name · Brand ·
Unit_Price_USD · Quantity · Total_Amount_USD · Customer_Segment · Region · Payment_Method · Is_Refund

**Regions:** North America · Europe · Asia Pacific · Vietnam · Southeast Asia · Middle East

**Intentional data quality issues in raw CSV:**
- Dates in 4 mixed formats (`%Y-%m-%d`, `%d/%m/%Y`, `%m-%d-%Y`, `%Y/%d/%m`)
- Prices with currency strings (`"199.99 USD"`, `"199.99 dollars"`, `"N/A"`)
- Quantities with unit strings (`"5 pcs"`) and negative values
- Totals that are `None`, contain `"dollars"`, or are miscalculated
- Brand / Category with inconsistent casing and occasional `None` values
- Refund flags as mixed types (`"True"`, `"1"`, `"yes"`, `""`, `None`)

See `docs/DATA_DICTIONARY.md` for full column descriptions.

---

## ETL Pipeline

**Script:** `etl/clean_data.py` — uses helpers from `etl/util.py`

Steps:
1. Parse `Transaction_Date` — tries all 4 known formats in order, `None` if no match
2. Normalise text columns to lowercase stripped strings
3. Clean `Unit_Price_USD` — strip currency symbols, convert to float, discard negatives
4. Clean `Quantity` — strip `"pcs"`, convert to int, discard negatives / zero
5. Normalise `Is_Refund` — map `"true"/"1"/"yes"` → `True`, `"false"/"0"/"no"` → `False`
6. Recalculate `Total_Amount_USD = Unit_Price_USD × Quantity`
7. Validate schema and report null counts
8. Export to `data_source/hardware_sales_2019_2025_clean.csv`

**`etl/util.py`** — reusable helpers used by `clean_data.py`:
- `parse_date()`, `parse_price()`, `parse_quantity()`, `parse_bool()`, `normalise_text()`
- `validate_dataframe()`, `report_nulls()`

---

## API Endpoints

Flask API runs at `http://127.0.0.1:5000`. All `/metrics/*` endpoints accept common
filter params: `category`, `brand`, `region`, `date_from`, `date_to`, `include_refund`.

| Method | Endpoint | Description |
|---|---|---|
| GET | `/` | Health check |
| GET | `/metrics/summary` | Total revenue, transactions, avg order, refund rate |
| GET | `/metrics/revenue` | Total revenue (single value) |
| GET | `/metrics/category` | Revenue grouped by product category |
| GET | `/metrics/brand` | Revenue grouped by brand |
| GET | `/metrics/monthly` | Revenue grouped by month (YYYY-MM) |
| GET | `/metrics/region` | Revenue grouped by region |
| GET | `/metrics/segment` | Revenue grouped by customer segment |
| GET | `/metrics/top-products` | Top N products by revenue (`n` param, default 10) |
| GET | `/metrics/date-range` | Min and max Transaction_Date in the dataset |
| GET | `/filter/combined` | Paginated transactions — all filters + keyword search |
| GET | `/filter/category` | Filter by category (paginated) |
| GET | `/filter/search` | Keyword search (paginated) |
| GET | `/filter/options` | Distinct values for all filter dropdowns |

Full documentation: `docs/API_DOCUMENTATION.md`

---

## Dashboard

Two-page dashboard. Open either file directly in a browser while the API is running.

### Page 1 — Summary (`web_client/components/summary.html`)
- 4 KPI cards: Total Revenue, Transactions, Avg Order Value, Refunds
- Filter panel: Category, Brand, Region, Date From/To, Include Refunds toggle, Reset
- **Zone 1 — Top Performers:** Top Brands (horizontal bar) + Top 10 Products (horizontal bar)
- **Zone 2 — Revenue Trend:** Monthly Revenue area chart (full width)
- **Zone 3 — Revenue Breakdown:** By Category + By Region + By Customer Segment (3 columns)
- All charts update subtitle to reflect active filter state

### Page 2 — Transactions (`web_client/components/transactions.html`)
- Same filter panel + keyword search with 400ms debounce
- Paginated transaction table — 100 rows/page, 11 columns including Refund status
- Click any row → modal popup with full transaction detail (all 12 fields)
- Default date range: last 12 months relative to max date in DB

Full documentation: `docs/DASHBOARD_FEATURES.md`

---

## How to Run

### 1. Install dependencies
```bash
pip install -r requirement.txt
```

### 2. Generate raw dataset
```bash
python data_source/data_generator.py
```

### 3. Run ETL
```bash
python etl/clean_data.py
```

### 4. Start Flask API
```bash
cd api
python app.py
```
API available at `http://127.0.0.1:5000`

### 5. Open dashboard
Open `web_client/components/summary.html` or `web_client/components/transactions.html`
in your browser. No web server needed.

> After re-running the ETL, restart the Flask process to reload the clean CSV.

---

## Project Structure

```
Project_python_pandas_web/
│
├── data_source/
│   ├── data_generator.py
│   ├── hardware_sales_2019_2025_raw.csv
│   └── hardware_sales_2019_2025_clean.csv
│
├── etl/
│   ├── clean_data.py
│   └── util.py
│
├── api/
│   ├── app.py                    # Flask entry point + NaN-safe JSON provider
│   ├── services/
│   │   └── data_service.py       # Shared data loading helpers
│   └── routes/
│       ├── metrics.py            # /metrics/* endpoints
│       └── filters.py            # /filter/* endpoints
│
├── web_client/
│   ├── assets/
│   │   ├── css/style.css
│   │   └── js/
│   │       ├── api.js            # Fetch wrappers for all endpoints
│   │       ├── charts.js         # Highcharts renderers (6 charts)
│   │       ├── ui-shared.js      # Filter state, date defaults, shared helpers
│   │       ├── summary.js        # Summary page logic
│   │       └── transactions.js   # Transactions page logic + modal
│   └── components/
│       ├── summary.html          # Page 1: KPI cards + charts
│       └── transactions.html     # Page 2: table + search + detail modal
│
├── docs/
│   ├── API_DOCUMENTATION.md
│   ├── DASHBOARD_FEATURES.md
│   ├── DATA_DICTIONARY.md
│   └── FLOW_ARCHITECTURE.md
│
├── tests/
│   └── test_api.py
│
├── .gitignore
├── requirement.txt
└── README.md
```
