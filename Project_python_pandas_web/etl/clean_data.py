"""
clean_data.py
ETL pipeline: reads the raw CSV, cleans it using helpers from util.py,
validates the result, and writes the clean CSV.
"""

import os
import pandas as pd
from util import (
    parse_date,
    parse_price,
    parse_quantity,
    parse_bool,
    normalise_text,
    report_nulls,
    validate_dataframe,
)

current_folder = os.path.dirname(os.path.abspath(__file__))
raw_path   = os.path.join(current_folder, "..", "data_source", "hardware_sales_2019_2025_raw.csv")
clean_path = os.path.join(current_folder, "..", "data_source", "hardware_sales_2019_2025_clean.csv")

# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
print(f"Reading raw CSV: {raw_path}")
df = pd.read_csv(raw_path)
print(f"Loaded {len(df)} rows, {len(df.columns)} columns")

# ---------------------------------------------------------------------------
# Parse date
# ---------------------------------------------------------------------------
df["Transaction_Date"] = df["Transaction_Date"].apply(parse_date)

# ---------------------------------------------------------------------------
# Normalise text columns
# ---------------------------------------------------------------------------
text_cols = ["Product_Category", "Product_Name", "Brand",
             "Customer_Segment", "Region", "Payment_Method"]
for col in text_cols:
    if col in df.columns:
        df[col] = df[col].apply(normalise_text)

# ---------------------------------------------------------------------------
# Clean numeric columns
# ---------------------------------------------------------------------------
df["Unit_Price_USD"] = df["Unit_Price_USD"].apply(parse_price)
df["Quantity"]       = df["Quantity"].apply(parse_quantity)

# Recalculate total — discard original dirty values
df["Total_Amount_USD"] = df["Unit_Price_USD"] * df["Quantity"]

# ---------------------------------------------------------------------------
# Normalise boolean column
# ---------------------------------------------------------------------------
df["Is_Refund"] = df["Is_Refund"].apply(parse_bool)

# ---------------------------------------------------------------------------
# Validate & report nulls
# ---------------------------------------------------------------------------
validate_dataframe(df)
report_nulls(df)

# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------
df.to_csv(clean_path, index=False)
print(f"Clean CSV saved: {clean_path}")
