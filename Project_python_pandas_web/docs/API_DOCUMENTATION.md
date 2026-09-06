# API Documentation

**Base URL:** `http://127.0.0.1:5000`  
**Format:** All responses are JSON. `NaN` and `Infinity` float values are serialised as `null`.

---

## Common Filter Parameters

Accepted by all `/metrics/*` endpoints and `/filter/combined`:

| Parameter | Type | Description | Example |
|---|---|---|---|
| `category` | string | Product category (lowercase) | `cpu`, `gpu`, `ram` |
| `brand` | string | Brand name (lowercase) | `intel`, `amd`, `nvidia` |
| `region` | string | Sales region (lowercase) | `vietnam`, `europe`, `asia pacific` |
| `date_from` | string YYYY-MM-DD | Start date (inclusive) | `2024-01-01` |
| `date_to` | string YYYY-MM-DD | End date (inclusive) | `2024-12-31` |
| `include_refund` | string | Include refunded rows | `true` (default), `false` |

All text filter values are **case-insensitive** — internally normalised to lowercase.

---

## Metrics Endpoints

### GET /metrics/summary

KPIs for the filtered dataset.

**Response**
```json
{
  "total_revenue": 48320150.75,
  "total_transactions": 18432,
  "avg_order_value": 2622.51,
  "total_refunds": 1204,
  "refund_rate": 6.53
}
```

---

### GET /metrics/revenue

Total revenue as a single value.

```json
{ "total_revenue": 48320150.75 }
```

---

### GET /metrics/category

Revenue grouped by Product_Category, sorted descending.

```json
{ "gpu": 12450300.50, "cpu": 9820100.00, "monitor": 7340250.25 }
```

---

### GET /metrics/brand

Revenue grouped by Brand, sorted descending.

```json
{ "nvidia": 11200400.00, "intel": 9100200.50, "amd": 8350100.25 }
```

---

### GET /metrics/monthly

Revenue grouped by YYYY-MM, chronological order.

```json
{ "2024-01": 3820150.00, "2024-02": 4100300.50 }
```

---

### GET /metrics/region

Revenue grouped by Region, sorted descending.

```json
{
  "north america": 15200400.00,
  "europe": 12100300.50,
  "asia pacific": 9800200.25,
  "vietnam": 7500100.00,
  "southeast asia": 6200050.75,
  "middle east": 4100300.50
}
```

---

### GET /metrics/segment

Revenue grouped by Customer_Segment, sorted descending.

```json
{ "enterprise": 14200300.00, "wholesale": 11800250.50, "retail": 9500200.25 }
```

---

### GET /metrics/top-products

Top N products by total revenue.

**Extra parameter:** `n` (int, default `10`) — number of products to return.

**Example:** `GET /metrics/top-products?n=5&category=gpu`

```json
{ "nvidia gpu model 512": 420300.00, "nvidia gpu model 388": 398100.50 }
```

---

### GET /metrics/date-range

Actual min and max Transaction_Date in the dataset. Used by the frontend to set default date filter values.

```json
{ "min_date": "2019-01-01", "max_date": "2025-12-31" }
```

No filter parameters accepted — always reflects the full dataset.

---

## Filter Endpoints

### GET /filter/combined

Primary endpoint for the transaction table. Supports all common filter params plus:

| Parameter | Type | Default | Max | Description |
|---|---|---|---|---|
| `keyword` | string | `""` | — | Search in Product_Name, Brand, Product_Category, Region |
| `limit` | int | `500` | `1000` | Records per page |
| `offset` | int | `0` | — | Records to skip |

**Example:** `GET /filter/combined?category=gpu&region=vietnam&date_from=2024-01-01&limit=100&offset=0`

**Response**
```json
{
  "total": 1842,
  "limit": 100,
  "offset": 0,
  "data": [
    {
      "Transaction_ID": "T-000042",
      "Transaction_Date": "2024-03-15",
      "Product_Category": "gpu",
      "Product_Name": "nvidia gpu model 512",
      "Brand": "nvidia",
      "Unit_Price_USD": 899.99,
      "Quantity": 2,
      "Total_Amount_USD": 1799.98,
      "Customer_Segment": "retail",
      "Region": "vietnam",
      "Payment_Method": "credit card",
      "Is_Refund": false
    }
  ]
}
```

| Field | Type | Description |
|---|---|---|
| `total` | int | Total matching rows (before pagination) |
| `limit` | int | Page size applied |
| `offset` | int | Offset applied |
| `data` | array | Records for current page |

---

### GET /filter/category

Filter by product category. Paginated.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `category` | string | `""` | Empty = return all |
| `limit` | int | `500` | Max 1000 |
| `offset` | int | `0` | — |

Response: same structure as `/filter/combined`.

---

### GET /filter/search

Keyword search across Product_Name, Brand, Product_Category, Region. Paginated.

| Parameter | Type | Description |
|---|---|---|
| `keyword` | string | Search term (case-insensitive) |
| `limit` | int | Max 1000, default 500 |
| `offset` | int | Default 0 |

Returns `{ total: 0, data: [] }` when keyword is empty.

---

### GET /filter/options

Distinct values for all filterable dimensions. Used to populate dropdowns.

```json
{
  "categories": ["case", "cpu", "fan", "gpu", "hdd", "keyboard", "monitor",
                 "motherboard", "mouse", "power supply", "ram", "ssd", "vga"],
  "brands":     ["amd", "asus", "corsair", "dell", "g.skill", "gigabyte",
                 "hp", "intel", "logitech", "msi", "nvidia", "razer", "samsung"],
  "regions":    ["asia pacific", "europe", "middle east", "north america",
                 "southeast asia", "vietnam"],
  "payments":   ["bank transfer", "cash", "cod", "credit card", "e-wallet"]
}
```

---

## Health Check

### GET /

```json
{ "status": "API running" }
```

---

## Error Handling

| HTTP Status | Meaning |
|---|---|
| `200` | Success |
| `500` | Server error — check Flask console output |

Unrecognised filter values return `{ total: 0, data: [] }` — the filter simply yields zero matches rather than an error.

---

## Notes

- Date fields in responses are **ISO 8601 strings** (`YYYY-MM-DD`). `NaT` → `null`.
- Numeric fields return `null` when the source value was missing or unparseable.
- The API loads the clean CSV **once at startup**. Restart Flask after re-running `etl/clean_data.py`.
- `float NaN` and `Infinity` are intercepted by a custom JSON provider in `app.py` and serialised as `null` to ensure valid JSON output.
