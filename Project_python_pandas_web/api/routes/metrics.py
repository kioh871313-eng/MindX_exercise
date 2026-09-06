from flask import Blueprint, request, jsonify
import pandas as pd
import os

metrics_bp = Blueprint("metrics", __name__)

# ---------------------------------------------------------------------------
# Load & parse CSV once at startup
# ---------------------------------------------------------------------------
_data_path = os.path.join(
    os.path.dirname(__file__), "..", "..", "data_source",
    "hardware_sales_2019_2025_clean.csv"
)
_df = pd.read_csv(_data_path)
_df["Transaction_Date"] = pd.to_datetime(_df["Transaction_Date"], errors="coerce")

for _col in ["Product_Category", "Product_Name", "Brand",
             "Customer_Segment", "Region", "Payment_Method"]:
    if _col in _df.columns:
        _df[_col] = _df[_col].astype(str).str.strip().str.lower().replace({"nan": None})

def _parse_bool(v):
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None

if "Is_Refund" in _df.columns:
    _df["Is_Refund"] = _df["Is_Refund"].apply(_parse_bool)


# ---------------------------------------------------------------------------
# Internal filter helper (same logic as filters.py)
# ---------------------------------------------------------------------------
def _apply_filters(df, category="", brand="", region="",
                   date_from="", date_to="", include_refund=True):
    if category:
        df = df[df["Product_Category"] == category.lower()]
    if brand:
        df = df[df["Brand"] == brand.lower()]
    if region:
        df = df[df["Region"] == region.lower()]
    if date_from:
        try:
            df = df[df["Transaction_Date"] >= pd.to_datetime(date_from)]
        except Exception:
            pass
    if date_to:
        try:
            df = df[df["Transaction_Date"] <= pd.to_datetime(date_to)]
        except Exception:
            pass
    if not include_refund:
        df = df[df["Is_Refund"] != True]  # noqa: E712
    return df


def _get_filters():
    """Extract common filter params from the current request."""
    return dict(
        category       = request.args.get("category", ""),
        brand          = request.args.get("brand", ""),
        region         = request.args.get("region", ""),
        date_from      = request.args.get("date_from", ""),
        date_to        = request.args.get("date_to", ""),
        include_refund = request.args.get("include_refund", "true").lower() != "false",
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@metrics_bp.get("/summary")
def summary():
    """
    GET /metrics/summary
    Returns: total_revenue, total_transactions, avg_order_value, total_refunds, refund_rate
    All values respect filter params.
    """
    df = _apply_filters(_df, **_get_filters())

    total_revenue      = round(float(df["Total_Amount_USD"].sum()), 2)
    total_transactions = int(len(df))
    avg_order_value    = round(float(df["Total_Amount_USD"].mean()), 2) if total_transactions else 0.0
    total_refunds      = int((df["Is_Refund"] == True).sum())  # noqa: E712
    refund_rate        = round(total_refunds / total_transactions * 100, 2) if total_transactions else 0.0

    return jsonify({
        "total_revenue":       total_revenue,
        "total_transactions":  total_transactions,
        "avg_order_value":     avg_order_value,
        "total_refunds":       total_refunds,
        "refund_rate":         refund_rate,
    })


@metrics_bp.get("/revenue")
def revenue():
    """GET /metrics/revenue — total revenue (filter-aware)."""
    df = _apply_filters(_df, **_get_filters())
    return jsonify({"total_revenue": round(float(df["Total_Amount_USD"].sum()), 2)})


@metrics_bp.get("/category")
def revenue_by_category():
    """GET /metrics/category — revenue grouped by Product_Category."""
    df = _apply_filters(_df, **_get_filters())
    result = (
        df.groupby("Product_Category")["Total_Amount_USD"]
        .sum()
        .round(2)
        .sort_values(ascending=False)
    )
    return jsonify(result.to_dict())


@metrics_bp.get("/brand")
def revenue_by_brand():
    """GET /metrics/brand — revenue grouped by Brand."""
    df = _apply_filters(_df, **_get_filters())
    result = (
        df.groupby("Brand")["Total_Amount_USD"]
        .sum()
        .round(2)
        .sort_values(ascending=False)
    )
    return jsonify(result.to_dict())


@metrics_bp.get("/monthly")
def monthly_revenue():
    """GET /metrics/monthly — revenue grouped by YYYY-MM."""
    df = _apply_filters(_df, **_get_filters()).copy()
    df["Month"] = df["Transaction_Date"].dt.strftime("%Y-%m")
    result = df.groupby("Month")["Total_Amount_USD"].sum().round(2)
    return jsonify(result.to_dict())


@metrics_bp.get("/region")
def revenue_by_region():
    """GET /metrics/region — revenue grouped by Region."""
    df = _apply_filters(_df, **_get_filters())
    result = (
        df.groupby("Region")["Total_Amount_USD"]
        .sum()
        .round(2)
        .sort_values(ascending=False)
    )
    return jsonify(result.to_dict())


@metrics_bp.get("/top-products")
def top_products():
    """
    GET /metrics/top-products?n=10
    Returns top N products by total revenue.
    """
    df  = _apply_filters(_df, **_get_filters())
    n   = int(request.args.get("n", 10))
    result = (
        df.groupby("Product_Name")["Total_Amount_USD"]
        .sum()
        .round(2)
        .sort_values(ascending=False)
        .head(n)
    )
    return jsonify(result.to_dict())


@metrics_bp.get("/date-range")
def date_range():
    """
    GET /metrics/date-range
    Returns the min and max Transaction_Date in the dataset.
    Used by the frontend to set default date filter values.
    """
    min_date = _df["Transaction_Date"].min()
    max_date = _df["Transaction_Date"].max()
    return jsonify({
        "min_date": min_date.strftime("%Y-%m-%d") if pd.notna(min_date) else None,
        "max_date": max_date.strftime("%Y-%m-%d") if pd.notna(max_date) else None,
    })


@metrics_bp.get("/segment")
def revenue_by_segment():
    """GET /metrics/segment — revenue grouped by Customer_Segment."""
    df = _apply_filters(_df, **_get_filters())
    result = (
        df.groupby("Customer_Segment")["Total_Amount_USD"]
        .sum()
        .round(2)
        .sort_values(ascending=False)
    )
    return jsonify(result.to_dict())
