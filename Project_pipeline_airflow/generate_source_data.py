"""
generate_source_data.py
-----------------------
Generates a realistic (and intentionally messy) Computer Hardware Sales CSV
to simulate the kind of raw S3 source data the pipeline is built to handle.

Intentional data issues injected (noted in README for reference):
  - Duplicate rows (same order_id appearing 2-3 times)
  - NULL values in key columns (order_id, customer_id, product_id)
  - NULL values in measure columns (quantity, unit_price, profit)
  - Inconsistent date formats (YYYY-MM-DD vs MM/DD/YYYY vs DD-Mon-YYYY)
  - Inconsistent text casing (e.g. "CONSUMER" vs "consumer" vs "Consumer")
  - Whitespace padding in string columns ("  Laptop  ")
  - Negative values in quantity and price (data entry errors)
  - Total_revenue that doesn't match unit_price * quantity (calculation errors)
  - Mixed number formats ("1,250.00" vs "1250.0")
  - Extra/unexpected columns not in the schema
  - Empty strings instead of NULL

Usage:
  python generate_source_data.py [--rows 15000] [--output data/]

Output:
  data/hardware_sales_YYYYMMDD.csv
"""

import argparse
import os
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Seed for reproducibility (remove for truly random output each run)
# ---------------------------------------------------------------------------
RANDOM_SEED = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

# ---------------------------------------------------------------------------
# Reference data — Computer Hardware domain
# ---------------------------------------------------------------------------

CATEGORIES = {
    "Laptops": {
        "sub_categories": ["Gaming Laptops", "Business Laptops", "Ultrabooks", "Workstations"],
        "brands": ["Dell", "HP", "Lenovo", "Apple", "ASUS", "Acer", "MSI", "Razer"],
        "price_range": (400, 4500),
    },
    "Desktops": {
        "sub_categories": ["Gaming Desktops", "Business Desktops", "All-in-One", "Mini PCs"],
        "brands": ["Dell", "HP", "Lenovo", "Apple", "ASUS", "Acer", "CyberPowerPC"],
        "price_range": (350, 3500),
    },
    "Monitors": {
        "sub_categories": ["4K Monitors", "Gaming Monitors", "Business Monitors", "Curved Monitors"],
        "brands": ["LG", "Samsung", "Dell", "BenQ", "ASUS", "Acer", "ViewSonic"],
        "price_range": (120, 1800),
    },
    "Processors": {
        "sub_categories": ["Intel Core i3", "Intel Core i5", "Intel Core i7", "Intel Core i9", "AMD Ryzen 5", "AMD Ryzen 7", "AMD Ryzen 9"],
        "brands": ["Intel", "AMD"],
        "price_range": (80, 850),
    },
    "Memory (RAM)": {
        "sub_categories": ["DDR4", "DDR5", "SO-DIMM DDR4", "SO-DIMM DDR5"],
        "brands": ["Corsair", "Kingston", "G.Skill", "Crucial", "Samsung", "SK Hynix"],
        "price_range": (25, 350),
    },
    "Storage": {
        "sub_categories": ["SSD NVMe", "SSD SATA", "HDD", "External SSD", "External HDD"],
        "brands": ["Samsung", "WD", "Seagate", "Crucial", "Kingston", "Sabrent"],
        "price_range": (40, 600),
    },
    "Graphics Cards": {
        "sub_categories": ["NVIDIA GeForce", "AMD Radeon", "NVIDIA Quadro", "Intel Arc"],
        "brands": ["NVIDIA", "AMD", "ASUS", "MSI", "Gigabyte", "EVGA", "Zotac"],
        "price_range": (150, 2500),
    },
    "Networking": {
        "sub_categories": ["Routers", "Network Switches", "WiFi Adapters", "Network Cards"],
        "brands": ["Cisco", "TP-Link", "Netgear", "ASUS", "Ubiquiti", "D-Link"],
        "price_range": (20, 800),
    },
    "Peripherals": {
        "sub_categories": ["Keyboards", "Mice", "Webcams", "Headsets", "Speakers"],
        "brands": ["Logitech", "Razer", "Corsair", "SteelSeries", "HyperX", "Microsoft"],
        "price_range": (15, 400),
    },
    "Power Supplies": {
        "sub_categories": ["Modular PSU", "Semi-Modular PSU", "Non-Modular PSU", "UPS"],
        "brands": ["Corsair", "EVGA", "Seasonic", "be quiet!", "Thermaltake", "APC"],
        "price_range": (40, 350),
    },
}

CUSTOMER_SEGMENTS = ["Consumer", "Corporate", "Home Office", "Small Business", "Enterprise"]

GEOGRAPHIES = [
    ("United States", "North America", "California", "Los Angeles", "90001"),
    ("United States", "North America", "California", "San Francisco", "94105"),
    ("United States", "North America", "Texas", "Houston", "77001"),
    ("United States", "North America", "Texas", "Austin", "78701"),
    ("United States", "North America", "New York", "New York City", "10001"),
    ("United States", "North America", "Florida", "Miami", "33101"),
    ("United States", "North America", "Illinois", "Chicago", "60601"),
    ("United States", "North America", "Washington", "Seattle", "98101"),
    ("Canada", "North America", "Ontario", "Toronto", "M5H 2N2"),
    ("Canada", "North America", "British Columbia", "Vancouver", "V6B 2W9"),
    ("United Kingdom", "Europe", "England", "London", "EC1A 1BB"),
    ("United Kingdom", "Europe", "Scotland", "Edinburgh", "EH1 1YZ"),
    ("Germany", "Europe", "Bavaria", "Munich", "80331"),
    ("Germany", "Europe", "Berlin", "Berlin", "10115"),
    ("France", "Europe", "Île-de-France", "Paris", "75001"),
    ("Australia", "Asia Pacific", "New South Wales", "Sydney", "2000"),
    ("Australia", "Asia Pacific", "Victoria", "Melbourne", "3000"),
    ("Japan", "Asia Pacific", "Tokyo", "Tokyo", "100-0001"),
    ("Singapore", "Asia Pacific", "Central Region", "Singapore", "018956"),
    ("India", "Asia", "Maharashtra", "Mumbai", "400001"),
    ("India", "Asia", "Karnataka", "Bangalore", "560001"),
    ("Brazil", "South America", "São Paulo", "São Paulo", "01310-100"),
    ("Mexico", "North America", "CDMX", "Mexico City", "06600"),
]

SALES_CHANNELS = ["Online", "Retail Store", "Reseller", "Direct Sales", "Marketplace"]
ORDER_STATUSES = ["Completed", "Shipped", "Processing", "Returned", "Cancelled"]
SHIP_MODES = ["Standard Class", "Second Class", "First Class", "Same Day"]

FIRST_NAMES = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
               "William", "Barbara", "David", "Susan", "Richard", "Jessica", "Joseph", "Sarah",
               "Thomas", "Karen", "Wei", "Yuki", "Ahmed", "Priya", "Carlos", "Ana", "Lars"]
LAST_NAMES  = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
               "Wilson", "Taylor", "Chen", "Tanaka", "Patel", "Santos", "Müller", "Nguyen",
               "Kim", "Lee", "Martinez", "Anderson", "Thompson", "White", "Harris", "Clark"]


def _random_name() -> str:
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def _random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def _format_date(dt: datetime, style: int) -> str:
    """Return a date in one of several formats to simulate inconsistency."""
    if style == 0:
        return dt.strftime("%Y-%m-%d")
    elif style == 1:
        return dt.strftime("%m/%d/%Y")
    elif style == 2:
        return dt.strftime("%d-%b-%Y")   # e.g. 15-Jan-2024
    else:
        return dt.strftime("%d/%m/%Y")


def _random_id(prefix: str, n: int = 6) -> str:
    return f"{prefix}-{''.join(random.choices(string.digits, k=n))}"


# ---------------------------------------------------------------------------
# Core generation
# ---------------------------------------------------------------------------

def generate_clean_records(n: int, start_date: datetime, end_date: datetime) -> list[dict]:
    """Generate n clean sales records."""
    records = []

    # Pre-generate stable customer and product pools
    customers = [
        {
            "customer_id": _random_id("CUST", 5),
            "customer_name": _random_name(),
            "customer_segment": random.choice(CUSTOMER_SEGMENTS),
        }
        for _ in range(max(50, n // 100))
    ]

    products = []
    for category, meta in CATEGORIES.items():
        for sub_cat in meta["sub_categories"]:
            for brand in random.sample(meta["brands"], min(3, len(meta["brands"]))):
                low, high = meta["price_range"]
                unit_price = round(random.uniform(low, high), 2)
                unit_cost  = round(unit_price * random.uniform(0.45, 0.72), 2)
                products.append({
                    "product_id":   _random_id("PROD", 6),
                    "category":     category,
                    "sub_category": sub_cat,
                    "brand":        brand,
                    "product_name": f"{brand} {sub_cat} {_random_id('', 4)}",
                    "unit_price":   unit_price,
                    "unit_cost":    unit_cost,
                })

    for _ in range(n):
        cust    = random.choice(customers)
        prod    = random.choice(products)
        geo     = random.choice(GEOGRAPHIES)
        country, region, state, city, postal = geo

        order_date = _random_date(start_date, end_date)
        ship_days  = random.randint(1, 10)
        ship_date  = order_date + timedelta(days=ship_days)
        date_style = random.randint(0, 3)

        quantity  = random.randint(1, 20)
        discount  = round(random.choice([0, 0, 0, 0.05, 0.1, 0.15, 0.2]), 2)
        unit_price = prod["unit_price"]
        unit_cost  = prod["unit_cost"]
        total_rev  = round(unit_price * quantity * (1 - discount), 2)
        total_cost = round(unit_cost  * quantity, 2)
        profit     = round(total_rev - total_cost, 2)

        records.append({
            "order_id":          _random_id("ORD", 7),
            "order_date":        _format_date(order_date, date_style),
            "ship_date":         _format_date(ship_date, date_style),
            "ship_mode":         random.choice(SHIP_MODES),
            "customer_id":       cust["customer_id"],
            "customer_name":     cust["customer_name"],
            "customer_segment":  cust["customer_segment"],
            "country":           country,
            "city":              city,
            "state":             state,
            "postal_code":       postal,
            "region":            region,
            "product_id":        prod["product_id"],
            "category":          prod["category"],
            "sub_category":      prod["sub_category"],
            "product_name":      prod["product_name"],
            "brand":             prod["brand"],
            "unit_price":        unit_price,
            "unit_cost":         unit_cost,
            "quantity":          quantity,
            "discount":          discount,
            "total_revenue":     total_rev,
            "total_cost":        total_cost,
            "profit":            profit,
            "sales_channel":     random.choice(SALES_CHANNELS),
            "order_status":      random.choice(ORDER_STATUSES),
        })

    return records


def inject_data_issues(records: list[dict], issue_rate: float = 0.08) -> list[dict]:
    """
    Inject intentional data quality issues into a fraction of records.
    Issue types and their approximate distribution:
      - Duplicate rows            ~20% of issues
      - NULL in key columns       ~15%
      - NULL in measure columns   ~15%
      - Inconsistent casing       ~10%
      - Whitespace padding        ~10%
      - Negative values           ~10%
      - Revenue calculation error ~10%
      - Number format with commas ~5%
      - Extra unexpected column   ~3%
      - Empty string instead NULL ~2%
    """
    n_issues = int(len(records) * issue_rate)
    dirty    = records.copy()
    issue_log = []

    issue_pool = random.sample(range(len(dirty)), min(n_issues * 3, len(dirty)))

    idx = 0

    # -- Duplicates -----------------------------------------------------------
    n_dups = n_issues // 5
    for _ in range(n_dups):
        if idx >= len(issue_pool):
            break
        row = dirty[issue_pool[idx]].copy()
        dirty.append(row)                      # exact duplicate
        if random.random() < 0.5:             # slightly modified duplicate
            dup2 = row.copy()
            dup2["order_date"] = row["ship_date"]   # swapped date
            dirty.append(dup2)
        issue_log.append(f"duplicate: order_id={row['order_id']}")
        idx += 1

    # -- NULL in key columns --------------------------------------------------
    n_nulls = n_issues // 6
    key_cols = ["order_id", "customer_id", "product_id"]
    for _ in range(n_nulls):
        if idx >= len(issue_pool):
            break
        col = random.choice(key_cols)
        dirty[issue_pool[idx]][col] = None
        issue_log.append(f"null_key: {col}")
        idx += 1

    # -- NULL in measure columns ----------------------------------------------
    measure_cols = ["quantity", "unit_price", "total_revenue", "profit"]
    for _ in range(n_issues // 6):
        if idx >= len(issue_pool):
            break
        col = random.choice(measure_cols)
        dirty[issue_pool[idx]][col] = None
        issue_log.append(f"null_measure: {col}")
        idx += 1

    # -- Inconsistent casing --------------------------------------------------
    for _ in range(n_issues // 10):
        if idx >= len(issue_pool):
            break
        col = random.choice(["customer_segment", "region", "order_status", "sales_channel"])
        val = dirty[issue_pool[idx]].get(col, "")
        if isinstance(val, str):
            dirty[issue_pool[idx]][col] = val.upper() if random.random() < 0.5 else val.lower()
        issue_log.append(f"casing: {col}")
        idx += 1

    # -- Whitespace padding ---------------------------------------------------
    for _ in range(n_issues // 10):
        if idx >= len(issue_pool):
            break
        col = random.choice(["product_name", "customer_name", "city", "brand"])
        val = dirty[issue_pool[idx]].get(col, "")
        if isinstance(val, str):
            dirty[issue_pool[idx]][col] = f"  {val}  "
        issue_log.append(f"whitespace: {col}")
        idx += 1

    # -- Negative values ------------------------------------------------------
    for _ in range(n_issues // 10):
        if idx >= len(issue_pool):
            break
        col = random.choice(["quantity", "unit_price", "unit_cost"])
        val = dirty[issue_pool[idx]].get(col)
        if isinstance(val, (int, float)) and val is not None:
            dirty[issue_pool[idx]][col] = -abs(val)
        issue_log.append(f"negative: {col}")
        idx += 1

    # -- Revenue calculation mismatch -----------------------------------------
    for _ in range(n_issues // 10):
        if idx >= len(issue_pool):
            break
        dirty[issue_pool[idx]]["total_revenue"] = round(
            dirty[issue_pool[idx]].get("total_revenue", 100) * random.uniform(0.5, 2.0), 2
        )
        issue_log.append("calc_error: total_revenue")
        idx += 1

    # -- Number as formatted string with commas --------------------------------
    for _ in range(n_issues // 20):
        if idx >= len(issue_pool):
            break
        col = random.choice(["unit_price", "total_revenue"])
        val = dirty[issue_pool[idx]].get(col)
        if isinstance(val, float) and val is not None:
            dirty[issue_pool[idx]][col] = f"{val:,.2f}"   # e.g. "1,250.00"
        issue_log.append(f"formatted_number: {col}")
        idx += 1

    # -- Extra unexpected column ----------------------------------------------
    for _ in range(max(1, n_issues // 30)):
        if idx >= len(issue_pool):
            break
        dirty[issue_pool[idx]]["_extra_internal_flag"] = random.choice(["Y", "N", "PENDING"])
        issue_log.append("extra_column: _extra_internal_flag")
        idx += 1

    # -- Empty string instead of NULL -----------------------------------------
    for _ in range(max(1, n_issues // 50)):
        if idx >= len(issue_pool):
            break
        col = random.choice(["region", "state", "postal_code"])
        dirty[issue_pool[idx]][col] = ""
        issue_log.append(f"empty_string: {col}")
        idx += 1

    return dirty, issue_log


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate sample Computer Hardware Sales CSV data.")
    parser.add_argument("--rows",   type=int, default=12000, help="Number of clean base rows (default: 12000)")
    parser.add_argument("--output", type=str, default="data",  help="Output directory (default: data/)")
    parser.add_argument("--issue-rate", type=float, default=0.08, help="Fraction of rows with injected issues (default: 0.08)")
    parser.add_argument("--no-issues", action="store_true", help="Generate clean data only (no injected issues)")
    args = parser.parse_args()

    end_date   = datetime(2024, 12, 31)
    start_date = datetime(2022, 1, 1)

    print(f"Generating {args.rows:,} base records...")
    records = generate_clean_records(args.rows, start_date, end_date)

    if not args.no_issues:
        records, issue_log = inject_data_issues(records, issue_rate=args.issue_rate)
        print(f"Injected {len(issue_log):,} data issues across {int(args.rows * args.issue_rate):,} rows.")
    else:
        issue_log = []
        print("Skipping issue injection (--no-issues flag set).")

    # Shuffle to mix clean and dirty rows
    random.shuffle(records)

    df = pd.DataFrame(records)

    # Output
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    today_str  = datetime.today().strftime("%Y%m%d")
    output_path = output_dir / f"hardware_sales_{today_str}.csv"

    df.to_csv(output_path, index=False)

    print(f"\nOutput file : {output_path}")
    print(f"Total rows  : {len(df):,}")
    print(f"Columns     : {len(df.columns)} — {list(df.columns)}")
    print(f"\nNull counts per key column:")
    for col in ["order_id", "customer_id", "product_id"]:
        if col in df.columns:
            print(f"  {col}: {df[col].isnull().sum():,} NULLs")

    print(f"\nDuplicate order_ids: {df['order_id'].dropna().duplicated().sum():,}")

    if issue_log:
        issue_summary_path = output_dir / f"hardware_sales_{today_str}_issue_log.txt"
        with open(issue_summary_path, "w") as f:
            f.write("\n".join(issue_log))
        print(f"\nIssue log   : {issue_summary_path}")

    print("\nDone. Upload the CSV to your S3 bucket:")
    print(f"  aws s3 cp {output_path} s3://<your-bucket>/computer_hardware_sales/")


if __name__ == "__main__":
    main()
