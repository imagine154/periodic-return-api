# periodic_api.py — optimized version
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_compress import Compress
import pandas as pd
from periodic_return import fetch_nav_history, calculate_periodic_returns
import os

app = Flask(__name__)
CORS(app)
Compress(app)  # enable gzip compression

# ---------------------------------------------
# Load and preprocess dataset ONCE at startup
# ---------------------------------------------
DATA_FILE = "schemeswithcodes.pkl" if os.path.exists("schemeswithcodes.pkl") else "schemeswithcodes.csv"

print(f"📥 Loading {DATA_FILE} ...")
schemes_df = pd.read_pickle(DATA_FILE) if DATA_FILE.endswith(".pkl") else pd.read_csv(DATA_FILE)
print(f"✅ Loaded {len(schemes_df)} schemes.")

# Normalize text columns
for col in ["AMC", "schemeCategory", "schemeSubCategory", "Plan", "Option", "schemeName"]:
    schemes_df[col] = schemes_df[col].astype(str).str.strip()

# Add derived column for investment type
schemes_df["instrumentType"] = schemes_df["schemeSubCategory"].apply(
    lambda x: "ETF" if "ETF" in str(x).upper() else "Mutual Fund"
)


# ---------------------------------------------
# /api/stats - Metadata endpoint for dropdowns
# ---------------------------------------------
@app.route("/api/stats", methods=["GET"])
def get_metadata():
    df = schemes_df
    return jsonify({
        "total": len(df),
        "mutual_funds": int((df["instrumentType"] == "Mutual Fund").sum()),
        "etfs": int((df["instrumentType"] == "ETF").sum()),
        "amcs": sorted(df["AMC"].dropna().unique().tolist()),
        "categories": sorted(df["schemeCategory"].dropna().unique().tolist()),
        "subcategories": sorted(df["schemeSubCategory"].dropna().unique().tolist()),
        "plans": sorted(df["Plan"].dropna().unique().tolist()),
        "options": sorted(df["Option"].dropna().unique().tolist()),
    })


# ---------------------------------------------
# /api/schemes - Filtered or searched scheme list
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
    limit = int(request.args.get("limit", 500))  # safety limit

    df = schemes_df

    # Filter by investment type
    if selected_type.lower() != "both":
        df = df[df["instrumentType"].str.lower() == selected_type.lower()]

    # Apply dropdown filters
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

    # If no search or filters, skip large payload
    if not q and not amc_filter and not cat_filter and not subcat_filter:
        return jsonify([])

    # Apply name search
    if q:
        df = df[df["schemeName"].str.lower().str.contains(q)]

    df = df.sort_values(["AMC", "schemeCategory", "schemeSubCategory", "schemeName"])
    result = df.head(limit).to_dict(orient="records")

    return jsonify(result)


# ---------------------------------------------
# /api/periodic_returns - Return calculator
# ---------------------------------------------
@app.route("/api/periodic_returns", methods=["GET"])
def get_periodic_returns():
    amfi_code = request.args.get("code")
    if not amfi_code:
        return jsonify({"error": "Missing 'code' param"}), 400

    nav_df, scheme_name = fetch_nav_history(amfi_code)
    if nav_df is None or nav_df.empty:
        return jsonify({"error": f"No NAV data found for {amfi_code}"}), 404

    results = calculate_periodic_returns(nav_df)
    return jsonify({
        "scheme_name": scheme_name,
        "code": amfi_code,
        "results": results
    })


# ---------------------------------------------
# Optional utility route for Render debugging
# ---------------------------------------------
@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "total_schemes": len(schemes_df)})


# ---------------------------------------------
# Entry point
# ---------------------------------------------
if __name__ == "__main__":
    print("🚀 Flask app starting on port 5000 ...")
    app.run(debug=True, host="0.0.0.0", port=5000)
