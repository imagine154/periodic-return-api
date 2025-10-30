"""
periodic_api.py
Backend for Mutual Fund & ETF Return Analyzer
✅ Supports only 'Mutual Fund' and 'ETF'
✅ Fully compatible with rebuilt frontend (HTML)
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
from periodic_return import fetch_nav_history, calculate_periodic_returns

app = Flask(__name__)

# --------------------------------------------------------------------
# Enable CORS (Frontend URLs + local dev)
# --------------------------------------------------------------------
CORS(app, resources={
    r"/api/*": {
        "origins": [
            "https://smartequityinvest.in",
            "https://www.smartequityinvest.in",
            "http://localhost:5000",
            "http://127.0.0.1:5000"
        ],
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# --------------------------------------------------------------------
# Load master dataset once at startup
# --------------------------------------------------------------------
schemes_df = pd.read_csv("schemeswithcodes.csv")

# Add instrument type
schemes_df["instrumentType"] = schemes_df["schemeSubCategory"].apply(
    lambda x: "ETF" if "ETF" in str(x).upper() else "Mutual Fund"
)

# Normalize columns for consistent filtering
def normalize_series(series):
    return series.astype(str).str.strip().str.lower()

schemes_df["AMC_norm"] = normalize_series(schemes_df["AMC"])
schemes_df["Category_norm"] = normalize_series(schemes_df["schemeCategory"])
schemes_df["SubCategory_norm"] = normalize_series(schemes_df["schemeSubCategory"])
schemes_df["Plan_norm"] = normalize_series(schemes_df["Plan"])
schemes_df["Option_norm"] = normalize_series(schemes_df["Option"])

# --------------------------------------------------------------------
# Utility Helpers
# --------------------------------------------------------------------
def parse_multi_param(param_name):
    """Parse query parameters that may be comma-separated or repeated."""
    values = request.args.getlist(param_name)
    parsed = []
    for v in values:
        if "," in v:
            parsed.extend([x.strip() for x in v.split(",") if x.strip()])
        elif v.strip():
            parsed.append(v.strip())
    return parsed

def norm_list(vals):
    """Normalize to lowercase, strip spaces, and filter empties."""
    return [v.strip().lower() for v in vals if isinstance(v, str) and v.strip()]

# --------------------------------------------------------------------
# API: Schemes List
# --------------------------------------------------------------------
@app.route("/api/schemes", methods=["GET"])
def get_scheme_list():
    q = request.args.get("q", "").lower().strip()
    selected_type = request.args.get("type", "Mutual Fund")

    # Parse filters
    amc_filter = norm_list(parse_multi_param("amc"))
    cat_filter = norm_list(parse_multi_param("category"))
    subcat_filter = norm_list(parse_multi_param("subcategory"))
    plan_filter = norm_list(parse_multi_param("plan"))
    option_filter = norm_list(parse_multi_param("option"))

    df = schemes_df.copy()

    # --- Type filter ---
    if selected_type.lower() == "etf":
        df = df[df["instrumentType"].str.lower() == "etf"]
        df = df[df["Option_norm"] == "etf"]  # enforce ETF-only option
    else:
        df = df[df["instrumentType"].str.lower() == "mutual fund"]

    # --- Apply dropdown filters ---
    if amc_filter:
        df = df[df["AMC_norm"].apply(lambda x: any(v in x for v in amc_filter))]
    if cat_filter:
        df = df[df["Category_norm"].apply(lambda x: any(v in x for v in cat_filter))]
    if subcat_filter:
        df = df[df["SubCategory_norm"].apply(lambda x: any(v in x for v in subcat_filter))]
    if plan_filter and selected_type.lower() != "etf":
        df = df[df["Plan_norm"].apply(lambda x: any(v in x for v in plan_filter))]
    if option_filter:
        df = df[df["Option_norm"].apply(lambda x: any(v in x for v in option_filter))]

    # --- Search (optional) ---
    if q:
        q_norm = q.strip().lower()
        df = df[
            df["schemeName"].str.lower().str.contains(q_norm, na=False)
            | df["AMC"].str.lower().str.contains(q_norm, na=False)
            | df["schemeCategory"].str.lower().str.contains(q_norm, na=False)
            | df["schemeSubCategory"].str.lower().str.contains(q_norm, na=False)
            ]

    print(f"✅ Schemes: {len(df)} rows | Type={selected_type}")

    # Limit results for performance
    return jsonify(df.to_dict(orient="records"))

# --------------------------------------------------------------------
# API: Stats (Dropdown values)
# --------------------------------------------------------------------
@app.route("/api/stats", methods=["GET"])
def get_stats():
    df = schemes_df.copy()
    type_param = request.args.get("type", "Mutual Fund")
    plan_param = request.args.get("plan")
    option_param = request.args.get("option")

    if type_param.lower() == "etf":
        df = df[df["instrumentType"].str.lower() == "etf"]
        df = df[df["Option_norm"] == "etf"]
    else:
        df = df[df["instrumentType"].str.lower() == "mutual fund"]

    if plan_param and type_param.lower() != "etf":
        df = df[df["Plan_norm"] == plan_param.lower()]
    if option_param:
        df = df[df["Option_norm"] == option_param.lower()]

    stats = {
        "total": len(df),
        "mutual_funds": int((df["instrumentType"] == "Mutual Fund").sum()),
        "etfs": int((df["instrumentType"] == "ETF").sum()),
        "amcs": sorted(df["AMC"].dropna().unique().tolist()),
        "categories": sorted(df["schemeCategory"].dropna().unique().tolist()),
        "subcategories": sorted(df["schemeSubCategory"].dropna().unique().tolist()),
        "plans": sorted(df["Plan"].dropna().unique().tolist()),
        "options": sorted(df["Option"].dropna().unique().tolist()),
    }

    print(f"📊 Stats ({type_param}) — Total={stats['total']}")
    return jsonify(stats)

# --------------------------------------------------------------------
# API: Dependent Filters
# --------------------------------------------------------------------
@app.route("/api/dependent_filters", methods=["GET"])
def get_dependent_filters():
    selected_type = request.args.get("type", "Mutual Fund")
    amc_filter = norm_list(parse_multi_param("amc"))
    cat_filter = norm_list(parse_multi_param("category"))

    df = schemes_df.copy()
    if selected_type.lower() == "etf":
        df = df[df["instrumentType"].str.lower() == "etf"]
        df = df[df["Option_norm"] == "etf"]
    else:
        df = df[df["instrumentType"].str.lower() == "mutual fund"]

    if amc_filter:
        df = df[df["AMC_norm"].apply(lambda x: any(v in x for v in amc_filter))]
    if cat_filter:
        df = df[df["Category_norm"].apply(lambda x: any(v in x for v in cat_filter))]

    stats = {
        "categories": sorted(df["schemeCategory"].dropna().unique().tolist()),
        "subcategories": sorted(df["schemeSubCategory"].dropna().unique().tolist()),
        "plans": sorted(df["Plan"].dropna().unique().tolist()),
        "options": sorted(df["Option"].dropna().unique().tolist()),
    }

    return jsonify(stats)

# --------------------------------------------------------------------
# API: Periodic Returns
# --------------------------------------------------------------------
@app.route("/api/periodic_returns", methods=["GET"])
def get_periodic_returns_api():
    amfi_code = request.args.get("code")
    if not amfi_code:
        return jsonify({"error": "Missing 'code' param"}), 400

    nav_df, scheme_name = fetch_nav_history(amfi_code)
    if nav_df is None or nav_df.empty:
        return jsonify({"error": "Invalid or no NAV data found"}), 404

    results = calculate_periodic_returns(nav_df)
    return jsonify({
        "scheme_name": scheme_name,
        "code": amfi_code,
        "results": results
    })

# --------------------------------------------------------------------
# Root / Health Check
# --------------------------------------------------------------------
@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "message": "Mutual Fund & ETF Return Analyzer API is running",
        "endpoints": [
            "/api/stats",
            "/api/schemes",
            "/api/dependent_filters",
            "/api/periodic_returns?code=<scheme_code>",
        ]
    })

# --------------------------------------------------------------------
# Run Server
# --------------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
