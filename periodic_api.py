# periodic_api.py
"""
periodic_api.py
Backend for Mutual Fund & ETF Return Analyzer (DB-cached with CSV fallback)
- Preserves original behavior and endpoints from the user's provided file
- Adds DB caching for /api/stats, /api/schemes (optional), and /api/periodic_returns
- Admin endpoints: /api/precompute_all, /api/precache_filters
"""

import os
import traceback
from datetime import datetime, timezone

import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS

# import the compute functions exactly as provided
from periodic_return import fetch_nav_history, calculate_periodic_returns

# --------------------------------------------------------------------
# Try to import user's database.py (optional). If not available, operate in CSV-only mode.
# database.py is expected to expose helpers (any subset is fine):
#   - init_db()
#   - ensure_results_json_column()
#   - upsert_metadata(records)
#   - get_schemes_from_db(filters)
#   - get_filter_cache(type_)
#   - upsert_filter_cache(type_, data)
#   - get_precomputed_return_json(code) OR get_precomputed_return(code)
#   - upsert_fund_results_json(scheme_code, scheme_name, results_obj, meta=None)
#   - get_all_cached_returns(limit)
# --------------------------------------------------------------------
DB_AVAILABLE = False
try:
    from database import DB
    DB_AVAILABLE = True
    print("[periodic_api] database.py imported successfully")
except Exception as e:
    print("[periodic_api] database.py not found or failed to import. Falling back to CSV-only mode.")
    DB = None
    DB_AVAILABLE = False

# If DB present, initialize and ensure JSON column
if DB_AVAILABLE:
    try:
        if hasattr(DB, "init_db"):
            DB.init_db()
        if hasattr(DB, "ensure_results_json_column"):
            DB.ensure_results_json_column()
    except Exception as e:
        print("[periodic_api] Warning: DB init/ensure column failed:", e)

# --------------------------------------------------------------------
# App + CORS
# --------------------------------------------------------------------
app = Flask(__name__)

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
# Load master dataset once at startup (CSV fallback)
# --------------------------------------------------------------------
CSV_PATH = os.path.join(os.getcwd(), "schemeswithcodes.csv")
if not os.path.exists(CSV_PATH):
    raise FileNotFoundError(f"Required file missing: {CSV_PATH}")

schemes_df = pd.read_csv(CSV_PATH)

# Add instrument type (same logic you provided)
schemes_df["instrumentType"] = schemes_df["schemeSubCategory"].apply(
    lambda x: "ETF" if "ETF" in str(x).upper() else "Mutual Fund"
)

# Normalize columns for consistent filtering
def normalize_series(series):
    return series.astype(str).str.strip().str.lower()

# create normalized columns if not present
schemes_df["AMC_norm"] = normalize_series(schemes_df["AMC"])
schemes_df["Category_norm"] = normalize_series(schemes_df["schemeCategory"])
schemes_df["SubCategory_norm"] = normalize_series(schemes_df["schemeSubCategory"])
schemes_df["Plan_norm"] = normalize_series(schemes_df["Plan"])
schemes_df["Option_norm"] = normalize_series(schemes_df["Option"])

# If DB exists and has upsert_metadata, push CSV metadata once (best-effort)
if DB_AVAILABLE and hasattr(DB, "upsert_metadata") and hasattr(DB, "count_metadata"):
    try:
        current_count = DB.count_metadata()
        if current_count == 0:
            recs = []
            for _, row in schemes_df.iterrows():
                recs.append({
                    "scheme_code": row.get("schemeCode"),
                    "scheme_name": row.get("schemeName"),
                    "amc": row.get("AMC"),
                    "category": row.get("schemeCategory"),
                    "subcategory": row.get("schemeSubCategory"),
                    "plan": row.get("Plan"),
                    "option": row.get("Option"),
                    "type": row.get("instrumentType")
                })
            if recs:
                DB.upsert_metadata(recs)
                print(f"[periodic_api] metadata initialized in DB (count={len(recs)})")
        else:
            print(f"[periodic_api] metadata already loaded (count={current_count}), skipping upsert.")
    except Exception as e:
        print("[periodic_api] metadata upsert check failed:", e)


# --------------------------------------------------------------------
# Small helpers (preserve behavior from your original file)
# --------------------------------------------------------------------
def parse_multi_param(param_name):
    """Parse query parameters that may be comma-separated or repeated."""
    values = request.args.getlist(param_name)
    parsed = []
    for v in values:
        if not v:
            continue
        if "," in v:
            parsed.extend([x.strip() for x in v.split(",") if x.strip()])
        else:
            parsed.append(v.strip())
    return parsed

def norm_list(vals):
    """Normalize to lowercase, strip spaces, and filter empties."""
    return [v.strip().lower() for v in vals if isinstance(v, str) and v.strip()]

# --------------------------------------------------------------------
# Endpoint: /api/schemes (uses DB if available, else CSV)
# --------------------------------------------------------------------
@app.route("/api/schemes", methods=["GET"])
def get_scheme_list():
    try:
        q = request.args.get("q", "").lower().strip()
        selected_type = request.args.get("type", "Mutual Fund")

        # Parse filters (can be repeated or comma-separated)
        amc_filter = norm_list(parse_multi_param("amc"))
        cat_filter = norm_list(parse_multi_param("category"))
        subcat_filter = norm_list(parse_multi_param("subcategory"))
        plan_filter = norm_list(parse_multi_param("plan"))
        option_filter = norm_list(parse_multi_param("option"))

        filters = {
            "amc": [v for v in amc_filter if v],
            "category": [v for v in cat_filter if v],
            "subcategory": [v for v in subcat_filter if v],
            "plan": [v for v in plan_filter if v],
            "option": [v for v in option_filter if v],
        }

        # Include type for DB path (so DB filtering respects Mutual Fund vs ETF)
        if selected_type and selected_type.lower() != "both":
            filters["type"] = selected_type

        # Helper to normalise a row (DB row or CSV row) into frontend shape
        def normalise_row(r):
            # r can be a dict (DB RealDictRow) or a pandas Series (CSV)
            def get_field(keys, default=""):
                for k in keys:
                    if isinstance(r, dict):
                        if k in r and r[k] is not None:
                            return r[k]
                    else:
                        # pandas Series
                        if k in r.index and pd.notna(r[k]):
                            return r[k]
                return default

            scheme_code = str(get_field(["schemeCode", "scheme_code", "scheme_code".lower()], "")).strip()
            scheme_name = str(get_field(["schemeName", "scheme_name", "scheme_name".lower()], "")).strip()
            amc = str(get_field(["AMC", "amc"], "")).strip()
            category = str(get_field(["schemeCategory", "category"], "")).strip()
            subcategory = str(get_field(["schemeSubCategory", "subcategory"], "")).strip()
            plan = str(get_field(["Plan", "plan"], "")).strip()
            option = str(get_field(["Option", "option"], "")).strip()

            label = scheme_name or scheme_code
            if amc:
                label = f"{label} ({amc})"

            return {
                "value": scheme_code,
                "label": label,
                "schemeCode": scheme_code,
                "schemeName": scheme_name,
                "amc": amc,
                "category": category,
                "subcategory": subcategory,
                "plan": plan,
                "option": option
            }

        # If DB provides filtered fetch, prefer it
        if DB_AVAILABLE and hasattr(DB, "get_schemes_from_db"):
            try:
                rows = DB.get_schemes_from_db(filters)
                # Normalize all rows and return
                out = [normalise_row(dict(r)) for r in rows]
                # optional: apply search q (in case DB helper didn't)
                if q:
                    q_lower = q.lower()
                    out = [o for o in out if q_lower in (o.get("schemeName") or "").lower() or q_lower in (o.get("amc") or "").lower()]
                return jsonify(out)
            except Exception as e:
                print("[/api/schemes] DB get_schemes_from_db failed, falling back to CSV:", e)

        # CSV fallback
        df = schemes_df.copy()

        # Type filter
        if selected_type.lower() != "both":
            df = df[df["instrumentType"].str.lower().str.strip() == selected_type.lower().strip()]

        # safe_filter helper for CSV
        def safe_filter(df_local, column, values):
            if not values or column not in df_local.columns:
                return df_local
            norm_col = column + "_norm"
            if norm_col not in df_local.columns:
                df_local[norm_col] = df_local[column].astype(str).str.strip().str.lower()
            vals = [v.lower().strip() for v in values if v]
            return df_local[df_local[norm_col].apply(lambda x: any(v in x for v in vals))]

        df = safe_filter(df, "AMC", filters["amc"])
        df = safe_filter(df, "schemeCategory", filters["category"])
        df = safe_filter(df, "schemeSubCategory", filters["subcategory"])
        df = safe_filter(df, "Plan", filters["plan"])
        df = safe_filter(df, "Option", filters["option"])

        # Search q across CSV columns
        if q:
            q = q.lower()
            df = df[
                df["schemeName"].str.lower().str.contains(q, na=False)
                | df["AMC"].str.lower().str.contains(q, na=False)
                | df["schemeCategory"].str.lower().str.contains(q, na=False)
                | df["schemeSubCategory"].str.lower().str.contains(q, na=False)
                ]

        # Drop duplicates and normalize rows
        df = df.drop_duplicates(subset=["schemeCode"]).fillna("")
        out = []
        for _, row in df.iterrows():
            out.append(normalise_row(row))

        return jsonify(out)

    except Exception as e:
        print("❌ Error in /api/schemes:", str(e))
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# --------------------------------------------------------------------
# Endpoint: /api/stats (dropdown values) — DB-cached if possible
# --------------------------------------------------------------------
@app.route("/api/stats", methods=["GET"])
def get_stats():
    try:
        type_param = request.args.get("type", "Mutual Fund")
        plan_param = request.args.get("plan")
        option_param = request.args.get("option")

        # Try DB cache first
        if DB_AVAILABLE and hasattr(DB, "get_filter_cache"):
            try:
                cached = DB.get_filter_cache(type_param)
                if cached:
                    # cached likely contains JSONB arrays — convert where needed
                    resp = {
                        "total": cached.get("total") or 0,
                        "mutual_funds": int(cached.get("mutual_funds") or 0),
                        "etfs": int(cached.get("etfs") or 0),
                        "amcs": cached.get("amcs") or [],
                        "categories": cached.get("categories") or [],
                        "subcategories": cached.get("subcategories") or [],
                        "plans": cached.get("plans") or [],
                        "options": cached.get("options") or [],
                        "source": "db-cache"
                    }
                    return jsonify(resp)
            except Exception as e:
                print("[/api/stats] get_filter_cache failed, will compute fresh:", e)

        # Compute from CSV (existing logic)
        df = schemes_df.copy()
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
            "source": "fresh"
        }

        # Upsert into DB filter cache if helper exists
        if DB_AVAILABLE and hasattr(DB, "upsert_filter_cache"):
            try:
                DB.upsert_filter_cache(type_param, stats)
                print(f"📦 Cached filter stats for {type_param} into DB.")
            except Exception as e:
                print("[/api/stats] upsert_filter_cache failed:", e)

        print(f"📊 Stats ({type_param}) — Total={stats['total']}")
        return jsonify(stats)

    except Exception as e:
        print("❌ Error in /api/stats:", str(e))
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# --------------------------------------------------------------------
# Endpoint: /api/dependent_filters
# --------------------------------------------------------------------
@app.route("/api/dependent_filters", methods=["GET"])
def get_dependent_filters():
    """
    Returns dependent dropdown values based on selected filters:
    type (Mutual Fund / ETF), plan, amc, category, subcategory, option.
    Uses database metadata first, falls back to CSV if unavailable.
    """
    try:
        selected_type = request.args.get("type", "Mutual Fund")

        # Parse filters
        plan_filter = norm_list(parse_multi_param("plan"))
        amc_filter = norm_list(parse_multi_param("amc"))
        cat_filter = norm_list(parse_multi_param("category"))
        subcat_filter = norm_list(parse_multi_param("subcategory"))
        option_filter = norm_list(parse_multi_param("option"))

        filters = {
            "plan": plan_filter,
            "amc": amc_filter,
            "category": cat_filter,
            "subcategory": subcat_filter,
            "option": option_filter
        }

        # Ensure DB path respects selected type
        if selected_type and selected_type.lower() != "both":
            filters["type"] = selected_type

        df = None

        # ✅ Prefer DB if available
        if DB_AVAILABLE and hasattr(DB, "get_schemes_from_db"):
            try:
                rows = DB.get_schemes_from_db(filters)
                df = pd.DataFrame(rows)
                if not df.empty:
                    print(f"[/api/dependent_filters] Using DB metadata — {len(df)} records")
                    # Apply type filter defensively if column present
                    if "type" in df.columns and selected_type and selected_type.lower() != "both":
                        df = df[df["type"].astype(str).str.lower() == selected_type.lower()]
            except Exception as e:
                print(f"[/api/dependent_filters] DB metadata fetch failed: {e}")

        # ❌ Fallback to CSV if DB unavailable or empty
        if df is None or df.empty:
            df = schemes_df.copy()
            print("[/api/dependent_filters] Using CSV fallback")

            # Apply type filter (Mutual Fund / ETF)
            if selected_type.lower() == "etf":
                df = df[df["instrumentType"].str.lower() == "etf"]
            else:
                df = df[df["instrumentType"].str.lower() == "mutual fund"]

            # Normalize + filter helper (same as before)
            def safe_filter(df_local, column, values):
                if not values or column not in df_local.columns:
                    return df_local
                norm_col = column + "_norm"
                if norm_col not in df_local.columns:
                    df_local[norm_col] = df_local[column].astype(str).str.strip().str.lower()
                vals = [v.lower().strip() for v in values if v]
                return df_local[df_local[norm_col].apply(lambda x: any(v in x for v in vals))]

            df = safe_filter(df, "Plan", plan_filter)
            df = safe_filter(df, "AMC", amc_filter)
            df = safe_filter(df, "schemeCategory", cat_filter)
            df = safe_filter(df, "schemeSubCategory", subcat_filter)
            df = safe_filter(df, "Option", option_filter)

        # ✅ Build dependent dropdown lists dynamically
        amcs = sorted(df["amc"].dropna().unique().tolist()) if "amc" in df.columns else \
            sorted(df["AMC"].dropna().unique().tolist()) if "AMC" in df.columns else []
        categories = sorted(df["category"].dropna().unique().tolist()) if "category" in df.columns else \
            sorted(df["schemeCategory"].dropna().unique().tolist()) if "schemeCategory" in df.columns else []
        subcategories = sorted(df["subcategory"].dropna().unique().tolist()) if "subcategory" in df.columns else \
            sorted(df["schemeSubCategory"].dropna().unique().tolist()) if "schemeSubCategory" in df.columns else []
        options = sorted(df["option"].dropna().unique().tolist()) if "option" in df.columns else \
            sorted(df["Option"].dropna().unique().tolist()) if "Option" in df.columns else []
        plans = sorted(df["plan"].dropna().unique().tolist()) if "plan" in df.columns else \
            sorted(df["Plan"].dropna().unique().tolist()) if "Plan" in df.columns else []

        return jsonify({
            "amcs": amcs,
            "categories": categories,
            "subcategories": subcategories,
            "options": options,
            "plans": plans
        })

    except Exception as e:
        print("❌ Error in /api/dependent_filters:", e)
        traceback.print_exc()
        return jsonify({
            "amcs": [],
            "categories": [],
            "subcategories": [],
            "options": [],
            "plans": []
        }), 500


# --------------------------------------------------------------------
# Endpoint: /api/periodic_returns (DB cached with compute fallback)
# --------------------------------------------------------------------
@app.route("/api/periodic_returns", methods=["GET"])
def get_periodic_returns_api():
    try:
        amfi_code = request.args.get("code")
        if not amfi_code:
            return jsonify({"error": "Missing 'code' param"}), 400

        # 1) Try DB cached JSON (preferred)
        if DB_AVAILABLE:
            try:
                # prefer get_precomputed_return_json if available
                cached = None
                if hasattr(DB, "get_precomputed_return_json"):
                    cached = DB.get_precomputed_return_json(amfi_code)
                elif hasattr(DB, "get_precomputed_return"):
                    cached = DB.get_precomputed_return(amfi_code)
                if cached:
                    # if cached has results_json column, return it as-is
                    if isinstance(cached, dict) and "results_json" in cached and cached["results_json"]:
                        return jsonify({
                            "scheme_name": cached.get("scheme_name") or cached.get("schemeName"),
                            "code": cached.get("scheme_code") or cached.get("schemeCode") or amfi_code,
                            "results": cached.get("results_json"),
                            "source": "cache",
                            "updated_at": cached.get("updated_at")
                        })
                    # fallback mapping for legacy columns (return_1m etc.)
                    if isinstance(cached, dict) and cached.get("return_1m") is not None:
                        results = {
                            "1M": cached.get("return_1m"),
                            "3M": cached.get("return_3m"),
                            "6M": cached.get("return_6m"),
                            "1Y": cached.get("return_1y"),
                            "3Y": cached.get("return_3y"),
                            "5Y": cached.get("return_5y"),
                            "7Y": cached.get("return_7y"),
                            "10Y": cached.get("return_10y"),
                        }
                        return jsonify({
                            "scheme_name": cached.get("scheme_name"),
                            "code": cached.get("scheme_code"),
                            "results": results,
                            "source": "cache",
                            "updated_at": cached.get("updated_at")
                        })
            except Exception as e:
                print("[/api/periodic_returns] DB read failed (continuing to compute):", e)

        # 2) If not cached, compute using your periodic_return functions (exact same code you provided)
        nav_df, scheme_name = fetch_nav_history(amfi_code)
        if nav_df is None or nav_df.empty:
            return jsonify({"error": "Invalid or no NAV data found"}), 404

        results = calculate_periodic_returns(nav_df)

        # 3) Write to DB if possible (store entire results JSON in results_json)
        if DB_AVAILABLE and hasattr(DB, "upsert_fund_results_json"):
            try:
                meta = {
                    "type": None,
                    "plan": None,
                    "option": None
                }
                # try to look up scheme metadata from CSV to fill meta if present
                try:
                    row = schemes_df[schemes_df["schemeCode"] == amfi_code]
                    if not row.empty:
                        meta["type"] = row.iloc[0]["instrumentType"]
                        meta["plan"] = row.iloc[0].get("Plan")
                        meta["option"] = row.iloc[0].get("Option")
                except Exception:
                    pass

                DB.upsert_fund_results_json(amfi_code, scheme_name, results, meta=meta)
            except Exception as e:
                print("[/api/periodic_returns] Warning: DB upsert failed:", e)

        return jsonify({
            "scheme_name": scheme_name,
            "code": amfi_code,
            "results": results,
            "source": "fresh",
            "computed_at": datetime.now(timezone.utc).isoformat()
        })

    except Exception as e:
        print("❌ Error in /api/periodic_returns:", str(e))
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# --------------------------------------------------------------------
# Endpoint: /api/returns_summary - return a page-friendly sample of cached returns
# --------------------------------------------------------------------
@app.route("/api/returns_summary", methods=["GET"])
def returns_summary():
    try:
        limit = int(request.args.get("limit", 200))
        # if DB provides a helper to return precomputed cached returns, use it
        if DB_AVAILABLE and hasattr(DB, "get_all_cached_returns"):
            try:
                rows = DB.get_all_cached_returns(limit)
                # rows should contain scheme_code, scheme_name, results_json (or equivalent)
                out = []
                for r in rows:
                    if isinstance(r, dict):
                        res = r.get("results_json") or {}
                        out.append({
                            "scheme_code": r.get("scheme_code") or r.get("schemeCode"),
                            "scheme_name": r.get("scheme_name") or r.get("schemeName"),
                            "results": res,
                            "updated_at": r.get("updated_at")
                        })
                return jsonify(out)
            except Exception as e:
                print("[/api/returns_summary] DB helper failed, falling back:", e)

        # fallback: attempt to read fund_returns table via generic DB helper 'get_all_returns' if present
        if DB_AVAILABLE and hasattr(DB, "get_all_returns"):
            try:
                rows = DB.get_all_returns(limit)
                return jsonify(rows)
            except Exception as e:
                print("[/api/returns_summary] get_all_returns failed:", e)

        # last-resort: return empty list (no precomputed data)
        return jsonify([])

    except Exception as e:
        print("❌ Error in /api/returns_summary:", e)
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# --------------------------------------------------------------------
# Endpoint: /api/top_performers - return top N funds for each category
# --------------------------------------------------------------------
@app.route("/api/top_performers", methods=["GET"])
def top_performers():
    try:
        investment_type = request.args.get("type", "Mutual Fund")
        if investment_type == "Mutual Fund":
            plan = "Direct"
            option = "Growth"
            categories = [
                "Debt Scheme", "Equity Scheme", "Gold & Silver Scheme",
                "Hybrid Scheme", "Index Funds", "Solution Oriented Scheme"
            ]
        else:
            # use DB-real plan for ETFs (your DB shows Direct)
            plan = "Direct"
            option = "ETF"
            categories = ["Debt Scheme", "Equity Scheme", "Gold & Silver Scheme"]

        if not (DB_AVAILABLE and hasattr(DB, "get_top_performers")):
            return jsonify({"error": "Database not ready"}), 500

        results = {}
        for period in ["3Y", "5Y", "10Y"]:
            db_rows = DB.get_top_performers(investment_type, categories, period, plan, option)
            period_results = {}
            for row in db_rows:
                # row is likely a tuple/dict depending on cursor settings - adapt accordingly
                # assume cursor.fetchall() returns list of dict-like rows (psycopg2.extras.RealDictCursor)
                category = row["category"]
                if category not in period_results:
                    period_results[category] = []
                period_results[category].append({
                    "scheme_name": row["scheme_name"],
                    "return": float(row["return"]) if row["return"] is not None else None
                })
            if period_results:
                results[period] = period_results

        return jsonify(results)

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

    except Exception as e:
        print("❌ Error in /api/top_performers:", str(e))
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500



# --------------------------------------------------------------------
# Admin Endpoint: /api/precompute_all - compute & cache returns for all schemes in CSV (POST)
# --------------------------------------------------------------------
@app.route("/api/precompute_all", methods=["POST"])
def precompute_all():
    """
    Compute and cache returns for all schemes in manageable batches with retry,
    throttling, and database reconnection.
    Splits big batches into smaller chunks to avoid Render timeouts.

    Query params:
      - start: starting index (default 0)
      - batch: number of schemes to process (default 100)
    """

    import time, gc, requests, traceback
    from datetime import datetime

    try:
        start_index = int(request.args.get("start", 0))
        batch_size = min(int(request.args.get("batch", 100)), 10)
        end_index = start_index + batch_size

        # Split into sub-batches of 20 to avoid Render timeout
        mini_batch = 20

        codes = schemes_df["schemeCode"].dropna().unique().tolist()
        total_schemes = len(codes)
        batch_codes = codes[start_index:end_index]

        processed, failed = 0, []
        print(f"🧮 [{datetime.now().strftime('%H:%M:%S')}] Starting precompute: {start_index} → {end_index} / {total_schemes}")

        session = requests.Session()

        # Loop through mini-batches
        for sub_start in range(0, len(batch_codes), mini_batch):
            sub_codes = batch_codes[sub_start:sub_start + mini_batch]
            print(f"➡️  Processing sub-batch {sub_start} → {sub_start + len(sub_codes)} ...")

            for code in sub_codes:
                try:
                    scheme_name, results = None, None

                    # Retry NAV fetch up to 3 times
                    for attempt in range(3):
                        try:
                            nav_df, scheme_name = fetch_nav_history(code, session=session)
                            if nav_df is not None and not nav_df.empty:
                                results = calculate_periodic_returns(nav_df)
                                break
                            else:
                                wait_time = 2 * (attempt + 1)
                                print(f"⚠️ [{code}] Empty NAV data (attempt {attempt+1}/3). Retrying in {wait_time}s...")
                                time.sleep(wait_time)
                        except requests.exceptions.Timeout:
                            wait_time = 3 * (attempt + 1)
                            print(f"⏱️ [{code}] Timeout attempt {attempt+1}, retrying in {wait_time}s...")
                            time.sleep(wait_time)
                        except Exception as e:
                            wait_time = 2 * (attempt + 1)
                            print(f"⚠️ [{code}] fetch attempt {attempt+1} failed: {e}")
                            time.sleep(wait_time)

                    if results is None:
                        failed.append({"code": code, "reason": "no NAV after retries"})
                        continue

                    # --- Save to DB safely ---
                    if DB_AVAILABLE and hasattr(DB, "upsert_fund_results_json"):
                        try:
                            # reconnect if helper available
                            if hasattr(DB, "ensure_connection_alive"):
                                DB.ensure_connection_alive()

                            row = schemes_df[schemes_df["schemeCode"] == code]
                            meta = {}
                            if not row.empty:
                                meta = {
                                    "type": row.iloc[0]["instrumentType"],
                                    "plan": row.iloc[0].get("Plan"),
                                    "option": row.iloc[0].get("Option")
                                }

                            DB.upsert_fund_results_json(code, scheme_name, results, meta=meta)

                        except Exception as e:
                            if "closed" in str(e).lower():
                                print(f"🔁 [DB] reconnecting after closed connection for {code}")
                                try:
                                    import psycopg2
                                    from psycopg2.extras import RealDictCursor
                                    DB.conn = psycopg2.connect(DB.DATABASE_URL, sslmode="require")
                                    DB.cursor = DB.conn.cursor(cursor_factory=RealDictCursor)
                                    DB.upsert_fund_results_json(code, scheme_name, results, meta=meta)
                                except Exception as inner:
                                    print(f"💾 [DB] re-upsert failed for {code}: {inner}")
                                    failed.append({"code": code, "error": str(inner)})
                            else:
                                print(f"💾 [DB] upsert failed for {code}: {e}")
                                failed.append({"code": code, "error": str(e)})

                    processed += 1

                    # Log periodically
                    if processed % 25 == 0:
                        print(f"✅ [{datetime.now().strftime('%H:%M:%S')}] Processed {processed} schemes...")

                    # Free memory
                    del nav_df, results
                    gc.collect()
                    time.sleep(0.3)

                except Exception as e:
                    print(f"❌ [precompute_all] failed for {code}: {e}")
                    failed.append({"code": code, "error": str(e)})
                    time.sleep(0.5)

            # Cooldown between sub-batches
            print(f"⏸ Cooling 4s after sub-batch...")
            time.sleep(4)
            gc.collect()

        next_start = end_index if end_index < total_schemes else None
        print(f"🏁 [{datetime.now().strftime('%H:%M:%S')}] Batch done — processed={processed}, failed={len(failed)}")

        return jsonify({
            "message": "Batch precompute complete",
            "processed": processed,
            "failed_count": len(failed),
            "failed_sample": failed[:10],
            "next_start": next_start,
            "remaining": max(0, total_schemes - end_index),
            "total_schemes": total_schemes
        })

    except Exception as e:
        print("❌ Error in /api/precompute_all:", e)
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# --------------------------------------------------------------------
# Admin Endpoint: /api/precache_filters - compute & store filter cache in DB
# --------------------------------------------------------------------
@app.route("/api/precache_filters", methods=["POST"])
def precache_filters():
    try:
        if not (DB_AVAILABLE and hasattr(DB, "upsert_filter_cache")):
            return jsonify({"message": "DB filter cache helper not available"}), 400
        # compute for both types
        for t in ["Mutual Fund", "ETF"]:
            # reuse get_stats logic: call endpoint function directly to get fresh data (non-cached)
            with app.test_request_context(f"/api/stats?type={t}"):
                resp = get_stats()
                # get_stats already upserts to DB via upsert_filter_cache
        return jsonify({"message": "filter cache refreshed"})
    except Exception as e:
        print("❌ Error in /api/precache_filters:", e)
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

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
            "/api/returns_summary",
            "/api/top_performers?type=<investment_type>",
            "/api/precompute_all (POST)",
            "/api/precache_filters (POST)"
        ]
    })

# --------------------------------------------------------------------
# Run Server
# --------------------------------------------------------------------
if __name__ == "__main__":
    debug_mode = os.environ.get("DEBUG", "false").lower() == "true"
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=debug_mode, host="0.0.0.0", port=port)
