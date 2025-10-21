from flask import Flask, request, jsonify
import pandas as pd
from periodic_return import fetch_nav_history, calculate_periodic_returns
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Load enhanced dataset
schemes_df = pd.read_csv("schemeswithcodes.csv")

# Add derived column for instrument type
schemes_df["instrumentType"] = schemes_df["schemeSubCategory"].apply(
    lambda x: "ETF" if "ETF" in str(x).upper() else "Mutual Fund"
)

@app.route("/api/schemes", methods=["GET"])
def get_scheme_list():
    q = request.args.get("q", "").lower().strip()
    selected_type = request.args.get("type", "Mutual Fund")
    amc_filter = request.args.getlist("amc")
    cat_filter = request.args.getlist("category")
    subcat_filter = request.args.getlist("subcategory")
    plan_filter = request.args.getlist("plan")
    option_filter = request.args.getlist("option")

    df = schemes_df.copy()

    # Filter by type
    if selected_type != "Both":
        df = df[df["instrumentType"] == selected_type]

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

    # Search fund name
    if q:
        df = df[df["schemeName"].str.lower().str.contains(q)]

    return jsonify(df.head(100).to_dict(orient="records"))

@app.route("/api/periodic_returns", methods=["GET"])
def get_periodic_returns():
    amfi_code = request.args.get("code")
    if not amfi_code:
        return jsonify({"error": "Missing 'code' param"}), 400

    nav_df, scheme_name = fetch_nav_history(amfi_code)
    if nav_df is None or nav_df.empty:
        return jsonify({"error": "Invalid or no NAV data found"}), 404

    results = calculate_periodic_returns(nav_df)
    return jsonify({"scheme_name": scheme_name, "code": amfi_code, "results": results})

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
