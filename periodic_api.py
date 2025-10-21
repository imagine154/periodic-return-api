from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
from periodic_return import fetch_nav_history, calculate_periodic_returns

app = Flask(__name__)
CORS(app)

# Load the master fund data
schemes_df = pd.read_csv("schemeswithcodes.csv")


@app.route("/api/filters", methods=["GET"])
def get_filters():
    """Return unique filter values for dropdowns"""
    filters = {
        "AMC": sorted(schemes_df["AMC"].dropna().unique().tolist()),
        "schemeCategory": sorted(schemes_df["schemeCategory"].dropna().unique().tolist()),
        "schemeSubCategory": sorted(schemes_df["schemeSubCategory"].dropna().unique().tolist()),
        "Plan": ["Direct", "Regular"],
        "Option": ["Growth", "IDCW"]
    }
    return jsonify(filters)


@app.route("/api/schemes", methods=["GET"])
def get_scheme_list():
    """Return list of schemes based on filters and search query"""
    q = request.args.get("q", "").lower().strip()
    amc = request.args.getlist("amc")
    category = request.args.getlist("category")
    subcategory = request.args.getlist("subcategory")
    plan = request.args.getlist("plan")
    option = request.args.getlist("option")

    filtered = schemes_df.copy()

    if amc:
        filtered = filtered[filtered["AMC"].isin(amc)]
    if category:
        filtered = filtered[filtered["schemeCategory"].isin(category)]
    if subcategory:
        filtered = filtered[filtered["schemeSubCategory"].isin(subcategory)]
    if plan:
        filtered = filtered[filtered["Plan"].isin(plan)]
    if option:
        filtered = filtered[filtered["Option"].isin(option)]
    if q:
        filtered = filtered[filtered["schemeName"].str.lower().str.contains(q)]

    result = filtered.head(100).to_dict(orient="records")
    return jsonify(result)


@app.route("/api/periodic_returns", methods=["GET"])
def get_periodic_returns():
    """Calculate periodic returns for given AMFI codes"""
    amfi_codes = request.args.getlist("code")
    results = []

    for code in amfi_codes:
        df, scheme_name = fetch_nav_history(code)
        if df is None or df.empty:
            continue
        perf = calculate_periodic_returns(df)
        results.append({"scheme_name": scheme_name, "code": code, "returns": perf})

    return jsonify(results)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)