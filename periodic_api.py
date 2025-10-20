# periodic_api.py
from flask import Flask, request, jsonify
import pandas as pd
from periodic_return import fetch_nav_history, calculate_periodic_returns  # adjust names

from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # allow cross-origin requests (from your WordPress site)

# load scheme list
schemes_df = pd.read_csv("schemeswithcodes.csv")

@app.route("/api/schemes", methods=["GET"])
def get_scheme_list():
    q = request.args.get("q", "").lower().strip()
    if q:
        filtered = schemes_df[schemes_df["schemeName"].str.lower().str.contains(q, na=False)]
    else:
        filtered = schemes_df.head(20)

    # Replace NaN or non-serializable values with None
    clean_df = filtered.where(pd.notnull(filtered), None)

    # Convert to JSON-safe dict
    result = clean_df.head(50).to_dict(orient="records")

    return jsonify(result)

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
