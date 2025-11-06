import requests
import pandas as pd
from datetime import timedelta
import os

# -------------------------
# Config
# -------------------------
SIP_AMOUNT = 10000
SIP_DAY = 1
INPUT_FILE = "schemes.txt"
OUTPUT_FILE = "sip_periodic_returns.csv"
MFAPI_BASE = "https://api.mfapi.in/mf/"
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

PERIODS = {
    "1M": 30,
    "3M": 90,
    "6M": 180,
    "1Y": 365,
    "3Y": 365 * 3,
    "5Y": 365 * 5,
    "7Y": 365 * 7,
    "10Y": 365 * 10,
}

# -------------------------
# XIRR Function
# -------------------------
def xirr(cashflows, dates, guess=0.1):
    """Compute XIRR using Newton–Raphson method."""
    def npv(rate):
        return sum([
            cf / ((1 + rate) ** ((d - dates[0]).days / 365))
            for cf, d in zip(cashflows, dates)
        ])

    rate = guess
    for _ in range(200):
        f_value = npv(rate)
        f_derivative = sum([
            -cf * ((d - dates[0]).days / 365) /
            ((1 + rate) ** (((d - dates[0]).days / 365) + 1))
            for cf, d in zip(cashflows, dates)
        ])
        if f_derivative == 0:
            break
        new_rate = rate - f_value / f_derivative
        if abs(new_rate - rate) < 1e-8:
            return rate
        rate = new_rate
    return rate


# -------------------------
# Fetch NAV History
# -------------------------
def fetch_nav_history(amfi_numeric_code, session=None):
    """Fetch NAV history and return a cleaned DataFrame indexed by date."""
    url = MFAPI_BASE + str(int(amfi_numeric_code))
    try:
        r = requests.get(url, headers=HTTP_HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()

        if "data" not in data or not data["data"]:
            print(f"⚠️ No NAV data found for {amfi_numeric_code}")
            return None, None

        df = pd.DataFrame(data["data"])
        df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
        df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
        df = df.dropna(subset=["date", "nav"])
        df = df[df["nav"] > 0].sort_values("date").reset_index(drop=True)
        df = df.set_index("date")  # ✅ restore index-based selection (old behavior)

        scheme_name = data.get("meta", {}).get("scheme_name", str(amfi_numeric_code))
        return df, scheme_name

    except Exception as e:
        print(f"❌ Error fetching NAV for {amfi_numeric_code}: {e}")
        return None, None


# -------------------------
# SIP Simulation
# -------------------------
def simulate_sip(nav_df, start_date, end_date):
    """Simulate monthly SIP investment and compute portfolio value (auto-adjusts for stock splits)."""
    if nav_df is None or nav_df.empty:
        return None, None, None, None

    # Generate SIP schedule
    months = pd.date_range(start=start_date.replace(day=1), end=end_date, freq="MS")
    sip_dates = []
    for m in months:
        try:
            candidate = m.replace(day=SIP_DAY)
            if candidate <= end_date:
                sip_dates.append(candidate)
        except Exception:
            continue

    units, cashflows, dates = [], [], []

    # --- Detect stock splits dynamically ---
    nav_series = nav_df["nav"].sort_index().values
    split_factor = 1.0
    for i in range(1, len(nav_series)):
        prev, curr = nav_series[i - 1], nav_series[i]
        if prev > 0 and curr > 0:
            ratio = prev / curr
            # detect clean power-of-10 or 2 ratios (approximate to avoid float issues)
            for possible in [2, 5, 10, 50, 100]:
                if abs(ratio - possible) / possible < 0.05:  # within 5% tolerance
                    split_factor *= possible
                    print(f"🔧 Detected stock split ×{possible} at index {i} (NAV {prev:.2f} → {curr:.2f})")
                    # scale later NAVs upward to maintain continuity
                    nav_df.loc[nav_df.index[i]:, "nav"] *= possible
                    break

    # --- SIP simulation (normal logic) ---
    for d in sip_dates:
        df_sel = nav_df[nav_df.index >= d]
        if df_sel.empty:
            continue
        nav = float(df_sel["nav"].iloc[0])
        units.append(SIP_AMOUNT / nav)
        cashflows.append(-SIP_AMOUNT)
        dates.append(df_sel.index[0])

    if not units:
        return None, None, None, None

    total_units = sum(units)
    total_invested = len(units) * SIP_AMOUNT
    latest_nav = float(nav_df["nav"].iloc[-1])
    current_value = total_units * latest_nav

    # Redemption inflow
    cashflows.append(current_value)
    dates.append(nav_df.index[-1])

    return total_invested, current_value, dates, cashflows



# -------------------------
# Calculate Periodic Returns
# -------------------------
def calculate_periodic_returns(nav_df):
    """Calculate SIP absolute/XIRR returns for multiple durations."""
    if nav_df is None or nav_df.empty:
        return {}

    # 🧹 Ensure datetime index (fixes 'int - timedelta' issue)
    if "date" in nav_df.columns:
        nav_df["date"] = pd.to_datetime(nav_df["date"], errors="coerce")
        nav_df = nav_df.dropna(subset=["date"]).sort_values("date")
        nav_df = nav_df.set_index("date")

    if not pd.api.types.is_datetime64_any_dtype(nav_df.index):
        try:
            nav_df.index = pd.to_datetime(nav_df.index, errors="coerce")
        except Exception as e:
            print(f"⚠️ NAV index could not be converted to datetime: {e}")
            return {}

    nav_df = nav_df[~nav_df.index.isna()]
    if nav_df.empty:
        print("⚠️ Empty NAV data after date normalization.")
        return {}

    # ✅ Core computation
    end_date = nav_df.index[-1]
    first_date = nav_df.index[0]
    results = {}

    for label, days in PERIODS.items():
        start_date = end_date - timedelta(days=days)

        # ✅ Skip if insufficient history
        if start_date < first_date:
            results[label] = None
            continue

        invested, value, dates, cashflows = simulate_sip(nav_df, start_date, end_date)
        if invested is None:
            results[label] = None
            continue

        if label in ["1M", "3M", "6M", "1Y"]:
            returns = ((value / invested) - 1) * 100  # Absolute return
        else:
            returns = xirr(cashflows, dates) * 100  # XIRR for multi-year

        results[label] = round(returns, 2) if returns is not None else None

    return results


# -------------------------
# Main
# -------------------------
def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Create {INPUT_FILE} with one entry per line.")
        return

    with open(INPUT_FILE, "r") as f:
        scheme_codes = [ln.strip() for ln in f if ln.strip()]

    all_results = []
    for amfi_code in scheme_codes:
        print(f"\n🔍 Processing scheme: {amfi_code}")
        nav_df, scheme_name = fetch_nav_history(amfi_code)
        if nav_df is None or nav_df.empty:
            print(f"⚠️ No NAV data for {amfi_code}")
            continue

        res = {"Scheme Code": amfi_code, "Scheme Name": scheme_name}
        res.update(calculate_periodic_returns(nav_df))
        all_results.append(res)

    if all_results:
        df_out = pd.DataFrame(all_results)
        df_out.to_csv(OUTPUT_FILE, index=False)
        print(f"\n✅ Results saved to {OUTPUT_FILE}\n")
        print(df_out.to_string(index=False))
    else:
        print("⚠️ No valid results to save.")


# -------------------------
# Run
# -------------------------
if __name__ == "__main__":
    main()
