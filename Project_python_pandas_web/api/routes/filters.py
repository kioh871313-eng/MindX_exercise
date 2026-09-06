from flask import Blueprint, request, jsonify
import pandas as pd
import os

filters_bp = Blueprint("filters", __name__)

# ---------------------------------------------------------------------------
# Load & parse CSV once at startup
# ---------------------------------------------------------------------------
_data_path = os.path.join(
    os.path.dirname(__file__), "..", "..", "data_source",
    "hardware_sales_2019_2025_clean.csv"
)
_df = pd.read_csv(_data_path)
_df["Transaction_Date"] = pd.to_datetime(_df["Transaction_Date"], errors="coerce")

# Normalise text columns
for _col in ["Product_Category", "Product_Name", "Brand",
             "Customer_Segment", "Region", "Payment_Method"]:
    if _col in _df.columns:
        _df[_col] = _df[_col].astype(str).str.strip().str.lower().replace({"nan": None})

# Normalise Is_Refund to bool
def _parse_bool(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None

if "Is_Refund" in _df.columns:
    _df["Is_Refund"] = _df["Is_Refund"].apply(_parse_bool)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _apply_filters(df, category="", brand="", region="",
                   date_from="", date_to="", include_refund=True, keyword=""):
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
    if keyword:
        kw = keyword.lower()
        mask = (
            df["Product_Name"].str.contains(kw, na=False) |
            df["Brand"].str.contains(kw, na=False) |
            df["Product_Category"].str.contains(kw, na=False) |
            df["Region"].str.contains(kw, na=False)
        )
        df = df[mask]
    return df


def _serialize(df: pd.DataFrame) -> list:
    """Convert DataFrame to JSON-safe list of dicts. NaN/NaT → null."""
    out = df.copy()
    if "Transaction_Date" in out.columns:
        out["Transaction_Date"] = out["Transaction_Date"].dt.strftime("%Y-%m-%d")
    # Replace all NaN/NaT/None with None so Flask serialises them as JSON null
    out = out.where(pd.notna(out), other=None)
    # Catch any remaining float NaN that slipped through
    records = out.to_dict(orient="records")
    import math
    clean = []
    for row in records:
        clean.append({
            k: (None if isinstance(v, float) and math.isnan(v) else v)
            for k, v in row.items()
        })
    return clean


def _paginate(df, limit, offset):
    total = len(df)
    page_df = df.iloc[offset: offset + limit]
    return total, page_df


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@filters_bp.get("/category")
def by_category():
    """
    GET /filter/category?category=&limit=500&offset=0
    Backward-compatible: returns paginated results.
    """
    cat    = request.args.get("category", "")
    limit  = min(int(request.args.get("limit", 500)), 1000)
    offset = int(request.args.get("offset", 0))

    filtered = _apply_filters(_df, category=cat)
    total, page = _paginate(filtered, limit, offset)

    return jsonify({
        "total": total,
        "limit": limit,
        "offset": offset,
        "data": _serialize(page)
    })


@filters_bp.get("/search")
def search():
    """
    GET /filter/search?keyword=&limit=500&offset=0
    Backward-compatible keyword search.
    """
    keyword = request.args.get("keyword", "")
    limit   = min(int(request.args.get("limit", 500)), 1000)
    offset  = int(request.args.get("offset", 0))

    if not keyword:
        return jsonify({"total": 0, "limit": limit, "offset": offset, "data": []})

    filtered = _apply_filters(_df, keyword=keyword)
    total, page = _paginate(filtered, limit, offset)

    return jsonify({
        "total": total,
        "limit": limit,
        "offset": offset,
        "data": _serialize(page)
    })


@filters_bp.get("/combined")
def combined():
    """
    GET /filter/combined
    Query params:
      category, brand, region, date_from (YYYY-MM-DD), date_to (YYYY-MM-DD),
      include_refund (true/false), keyword, limit (max 1000), offset
    """
    category       = request.args.get("category", "")
    brand          = request.args.get("brand", "")
    region         = request.args.get("region", "")
    date_from      = request.args.get("date_from", "")
    date_to        = request.args.get("date_to", "")
    include_refund = request.args.get("include_refund", "true").lower() != "false"
    keyword        = request.args.get("keyword", "")
    limit          = min(int(request.args.get("limit", 500)), 1000)
    offset         = int(request.args.get("offset", 0))

    filtered = _apply_filters(
        _df,
        category=category,
        brand=brand,
        region=region,
        date_from=date_from,
        date_to=date_to,
        include_refund=include_refund,
        keyword=keyword,
    )

    total, page = _paginate(filtered, limit, offset)

    return jsonify({
        "total": total,
        "limit": limit,
        "offset": offset,
        "data": _serialize(page)
    })


@filters_bp.get("/options")
def options():
    """
    GET /filter/options
    Returns distinct values for each filterable dimension.
    Used to dynamically populate filter dropdowns.
    """
    def distinct(col):
        return sorted(_df[col].dropna().unique().tolist()) if col in _df.columns else []

    return jsonify({
        "categories": distinct("Product_Category"),
        "brands":     distinct("Brand"),
        "regions":    distinct("Region"),
        "payments":   distinct("Payment_Method"),
    })
