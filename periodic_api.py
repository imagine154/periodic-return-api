from flask import Flask, request, jsonify
import pandas as pd
from periodic_return import fetch_nav_history, calculate_periodic_returns
from flask_cors import CORS
from functools import lru_cache

app = Flask(__name__)
CORS(app)

# ---------------------------------------------
# Load dataset once (with caching)
# ---------------------------------------------
@lru_cache(maxsize=1)
def load_schemes_data():
    df = pd.read_csv("schemeswithcodes.csv")

    # Clean / normalize columns
    for col in ["AMC", "schemeCategory", "schemeSubCategory", "Plan", "Option", "schemeName"]:
        df[col] = df[col].astype(str).str.strip()

    # Add instrument type
    df["instrumentType"] = df["schemeSubCategory"].apply(
        lambda x: "ETF" if "ETF" in str(x).upper() else "Mutual Fund"
    )

    print(f"✅ Loaded {len(df)} total schemes.")
    return df


# ---------------------------------------------
# /api/schemes - Filtered Scheme List
# ---------------------------------------------
@app.route("/api/schemes", methods=["GET"])
def get_scheme_list():
    q = request.args.get("q", "").lower().strip()
    selected_type = request.args.get("type", "Mutual Fund")
    amc_filter = request.args.getlist("amc")
    cat_filter = request.args.getlist("category")
    subcat_filter = request.args.getlist("subcategory")
    plan_filter = request.args.getlist("plan")
    option_filter = request.args.getlist("option")
    limit = int(request.args.get("limit", 10000))  # can cap for safety

    df = load_schemes_data().copy()

    # Filter by Investment Type
    if selected_type.lower() != "both":
        df = df[df["instrumentType"].str.lower() == selected_type.lower()]

    # Apply Dropdown Filters
    if amc_filter:
        df = df[df["AMC"].isin(amc_filter)]
    if cat_filter:
        df = df[df["schemeCategory"].isin(cat_filter)]
    if subcat_filter:
        df = df[df["schemeSubCategory"].isin(subcat_filter)]
    if plan_filter:
        df = df[df["Plan"].isin(plan_filter)]
    if option_filter:
        df = df[df["Option"].isin(option_filter)]

    # Text Search
    if q:
        df = df[df["schemeName"].str.lower().str.contains(q)]

    # Sort alphabetically for UX consistency
    df = df.sort_values(by=["AMC", "schemeCategory", "schemeSubCategory", "schemeName"])

    # Return limited data (default: all)
    result = df.head(limit).to_dict(orient="records")
    return jsonify(result)


# ---------------------------------------------
# /api/periodic_returns - Get Fund Returns
# ---------------------------------------------
@app.route("/api/periodic_returns", methods=["GET"])
def get_periodic_returns():
    amfi_code = request.args.get("code")
    if not amfi_code:
        return jsonify({"error": "Missing 'code' param"}), 400

    nav_df, scheme_name = fetch_nav_history(amfi_code)
    if nav_df is None or nav_df.empty:
        return jsonify({"error": f"No NAV data found for code {amfi_code}"}), 404

    results = calculate_periodic_returns(nav_df)
    return jsonify({
        "scheme_name": scheme_name,
        "code": amfi_code,
        "results": results
    })


# ---------------------------------------------
# /api/stats - Metadata (optional)
# ---------------------------------------------
@app.route("/api/stats", methods=["GET"])
def get_metadata():
    df = load_schemes_data()
    total = len(df)
    mf_count = (df["instrumentType"] == "Mutual Fund").sum()
    etf_count = (df["instrumentType"] == "ETF").sum()
    amcs = sorted(df["AMC"].unique().tolist())
    categories = sorted(df["schemeCategory"].unique().tolist())
    subcategories = sorted(df["schemeSubCategory"].unique().tolist())
    return jsonify({
        "total": total,
        "mutual_funds": int(mf_count),
        "etfs": int(etf_count),
        "amcs": amcs,
        "categories": categories,
        "subcategories": subcategories
    })


# ---------------------------------------------
# Entry point
# ---------------------------------------------
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)