"""
util.py
Reusable helper utilities for the ETL pipeline.
"""

import re
import pandas as pd


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------
_DATE_FORMATS = [
    "%Y-%m-%d",   # 2024-03-15  (pandas default)
    "%d/%m/%Y",   # 15/03/2024
    "%m-%d-%Y",   # 03-15-2024
    "%Y/%d/%m",   # 2024/15/03
]


def parse_date(value) -> "pd.Timestamp | None":
    """
    Try each known date format in order and return the first that succeeds.
    Returns None when no format matches.
    """
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    s = str(value).strip()
    for fmt in _DATE_FORMATS:
        try:
            return pd.to_datetime(s, format=fmt)
        except (ValueError, TypeError):
            continue
    return None


# ---------------------------------------------------------------------------
# Price parsing
# ---------------------------------------------------------------------------
_CURRENCY_RE = re.compile(r"[^\d.]")


def parse_price(value) -> float | None:
    """
    Clean a messy price string and return a float.
    Handles: "199.99 USD", "199.99 dollars", "N/A", "", None.
    Returns None when unparseable or negative.
    """
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    cleaned = _CURRENCY_RE.sub("", str(value)).strip()
    if not cleaned:
        return None
    try:
        result = float(cleaned)
        return result if result >= 0 else None
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Quantity parsing
# ---------------------------------------------------------------------------
_QTY_RE = re.compile(r"[^\d]")


def parse_quantity(value) -> int | None:
    """
    Clean a messy quantity string and return an int.
    Handles: "5 pcs", "5", 5. Returns None when unparseable or <= 0.
    """
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    cleaned = _QTY_RE.sub("", str(value)).strip()
    if not cleaned:
        return None
    try:
        result = int(cleaned)
        return result if result > 0 else None
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Boolean parsing
# ---------------------------------------------------------------------------
def parse_bool(value) -> bool | None:
    """
    Convert messy boolean representations to Python bool or None.
    Truthy: "true", "1", "yes" — Falsy: "false", "0", "no"
    """
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    s = str(value).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None


# ---------------------------------------------------------------------------
# Text normalisation
# ---------------------------------------------------------------------------
def normalise_text(value) -> str | None:
    """Strip whitespace and lowercase a string. Returns None for blank/NaN."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    cleaned = str(value).strip().lower()
    return cleaned if cleaned and cleaned != "nan" else None


# ---------------------------------------------------------------------------
# DataFrame validation & null reporting
# ---------------------------------------------------------------------------
def report_nulls(df: pd.DataFrame) -> dict:
    """Print and return {column: null_count} for columns that have nulls."""
    null_counts = df.isnull().sum()
    null_counts = null_counts[null_counts > 0].to_dict()

    if null_counts:
        print("Null values found:")
        for col, count in null_counts.items():
            pct = count / len(df) * 100
            print(f"  {col:<25} {count:>5}  ({pct:.1f}%)")
    else:
        print("No null values found.")

    return null_counts


def validate_dataframe(df: pd.DataFrame) -> bool:
    """
    Run basic sanity checks. Prints warnings for any issues found.
    Returns True if all required columns are present and the DataFrame is non-empty.
    """
    required_cols = [
        "Transaction_ID", "Transaction_Date", "Product_Category",
        "Product_Name", "Brand", "Unit_Price_USD", "Quantity",
        "Total_Amount_USD", "Customer_Segment", "Region",
        "Payment_Method", "Is_Refund",
    ]

    if df.empty:
        print("ERROR: DataFrame is empty!")
        return False

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"ERROR: Missing columns: {missing}")
        return False

    if "Unit_Price_USD" in df.columns:
        neg = (df["Unit_Price_USD"] < 0).sum()
        if neg:
            print(f"WARNING: {neg} rows with negative Unit_Price_USD")

    if "Total_Amount_USD" in df.columns:
        neg = (df["Total_Amount_USD"] < 0).sum()
        if neg:
            print(f"WARNING: {neg} rows with negative Total_Amount_USD")

    print(f"Validation passed. Rows: {len(df)}")
    return True
