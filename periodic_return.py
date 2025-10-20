import requests
import pandas as pd
from datetime import datetime, timedelta
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
    "10Y": 365 * 10
}


# -------------------------
# XIRR Function
# -------------------------
def xirr(cashflows, dates, guess=0.1):
    """
    Compute XIRR using Newton-Raphson on irregular cashflows.
    cashflows: list of numbers (positive for inflow, negative for outflow)
    dates: list of datetime.date / datetime.datetime objects with same length
    """
    def npv(rate):
        return sum([
            cf / ((1 + rate) ** ((d - dates[0]).days / 365.0))
            for cf, d in zip(cashflows, dates)
        ])

    rate = guess
    for _ in range(200):
        f_value = npv(rate)
        f_derivative = sum([
            -cf * ((d - dates[0]).days / 365.0) /
            ((1 + rate) ** (((d - dates[0]).days / 365.0) + 1))
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
def fetch_nav_history(amfi_code):
    """Fetch NAV history from mfapi.in safely. Returns (df, scheme_name) or (None, None)."""
    try:
        url = f"{MFAPI_BASE}{amfi_code}"
        response = requests.get(url, headers=HTTP_HEADERS, timeout=10)

        if response.status_code != 200:
            print(f"⚠️ Error fetching NAV data for {amfi_code} (HTTP {response.status_code})")
            return None, None

        data = response.json()
        if "data" not in data or not data["data"]:
            print(f"⚠️ No NAV data found for {amfi_code}")
            return None, None

        nav_data = data["data"]
        df = pd.DataFrame(nav_data)
        # parse and coerce
        df["date"] = pd.to_datetime(df["date"], format="%d-%m-%Y", errors="coerce")
        df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
        df = df.dropna(subset=["date", "nav"])
        if df.empty:
            print(f"⚠️ NAV parsing produced empty DataFrame for {amfi_code}")
            return None, None

        df = df.sort_values("date").reset_index(drop=True)
        df = df.set_index("date")

        # ensure nav positive
        df = df[df["nav"] > 0]
        if df.empty:
            print(f"⚠️ All NAVs are non-positive for {amfi_code}")
            return None, None

        scheme_name = data.get("meta", {}).get("scheme_name") or f"Scheme {amfi_code}"
        return df, scheme_name

    except requests.exceptions.RequestException as e:
        print(f"❌ Network error fetching {amfi_code}: {e}")
        return None, None
    except ValueError as e:
        print(f"❌ JSON decode / value error for {amfi_code}: {e}")
        return None, None
    except Exception as e:
        print(f"❌ Unexpected error fetching {amfi_code}: {e}")
        return None, None


# -------------------------
# SIP Simulation
# -------------------------
def simulate_sip(nav_df, start_date, end_date):
    """
    Simulate monthly SIP from start_date to end_date on SIP_DAY each month.
    nav_df: DataFrame indexed by date with column 'nav' (index is Timestamp)
    Returns: (total_invested, current_value, dates, cashflows) or (None, None, None, None)
    """
    if nav_df is None or nav_df.empty:
        return None, None, None, None

    # build list of month-start datetimes from start to end
    # ensure start_date is date or datetime
    start_month = start_date.replace(day=1)
    months = pd.date_range(start=start_month, end=end_date, freq="MS")

    sip_dates = []
    for m in months:
        # try to set SIP_DAY, if invalid (e.g. Feb 30) use last day of month
        year = m.year
        month = m.month
        try:
            candidate = datetime(year, month, SIP_DAY)
        except ValueError:
            # fallback to last day of month
            next_month = (m + pd.offsets.MonthEnd(0)).to_pydatetime()
            candidate = datetime(next_month.year, next_month.month, next_month.day)
        if candidate.date() <= end_date.date():
            sip_dates.append(pd.Timestamp(candidate))

    units = []
    cashflows = []
    dates = []

    for d in sip_dates:
        # find first available NAV on or after SIP date
        df_sel = nav_df[nav_df.index >= d]
        if df_sel.empty:
            # no nav after this SIP date - skip
            continue
        nav = float(df_sel["nav"].iloc[0])
        if nav <= 0:
            # skip invalid nav
            continue
        units.append(SIP_AMOUNT / nav)
        cashflows.append(-SIP_AMOUNT)
        dates.append(df_sel.index[0].to_pydatetime())

    if not units:
        return None, None, None, None

    total_units = sum(units)
    total_invested = len(units) * SIP_AMOUNT
    latest_nav = float(nav_df["nav"].iloc[-1])
    current_value = total_units * latest_nav

    # Add redemption (positive inflow) at last available NAV date
    cashflows.append(current_value)
    dates.append(nav_df.index[-1].to_pydatetime())

    return total_invested, current_value, dates, cashflows


# -------------------------
# Calculate Returns for Periods
# -------------------------
def calculate_periodic_returns(df):
    """
    Calculate periodic SIP-style returns for 1Y, 3Y, 5Y, 10Y safely.
    Returns dict like {'1Y': 12.34, '3Y': 15.67, ...}
    """
    try:
        if df is None or df.empty:
            return {}

        results = {}
        end_date = df.index[-1]
        end_nav = float(df["nav"].iloc[-1])

        # Helper to calculate CAGR safely given a start_date in index
        def safe_cagr(start_date_index):
            try:
                start_nav = float(df.loc[start_date_index, "nav"])
                if start_nav <= 0 or end_nav <= 0:
                    return None
                years = (end_date - start_date_index).days / 365.0
                if years <= 0:
                    return None
                return ((end_nav / start_nav) ** (1 / years) - 1) * 100
            except Exception:
                return None

        # for each multi-year period, find first available date on/after the requested start
        for years in [1, 3, 5, 10]:
            start_date_candidate = end_date - timedelta(days=years * 365)
            # select data on or after the candidate start date
            df_range = df[df.index >= start_date_candidate]
            if df_range.empty:
                # no data for this period
                continue
            # require a minimum number of observations for robustness (approx trading days)
            if len(df_range) < 200:
                # skip if too short history for this multi-year calc
                continue
            start_index = df_range.index[0]
            cagr = safe_cagr(start_index)
            if cagr is not None:
                results[f"{years}Y"] = round(cagr, 2)

        return results

    except Exception as e:
        print(f"⚠️ Error calculating periodic returns: {e}")
        return {}


# -------------------------
# Main
# -------------------------
def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Create {INPUT_FILE} with one AMFI code per line (e.g. 119551).")
        return

    with open(INPUT_FILE, "r") as f:
        scheme_codes = [ln.strip() for ln in f if ln.strip()]

    all_results = []
    for amfi_code in scheme_codes:
        print(f"\n🔍 Processing scheme: {amfi_code}")
        nav_df, scheme_name = fetch_nav_history(amfi_code)
        if nav_df is None or nav_df.empty:
            print(f"⚠️ No NAV data for {amfi_code} - skipping.")
            continue

        # Calculate periodic returns (1Y,3Y,5Y,10Y)
        per_results = calculate_periodic_returns(nav_df)

        # Optionally calculate SIP XIRR for a few windows (example 1Y,3Y)
        sip_results = {}
        try:
            # Example: compute SIP xirr for 1Y period if enough data
            end_date = nav_df.index[-1]
            start_1y = end_date - timedelta(days=365)
            df_1y = nav_df[nav_df.index >= start_1y]
            if not df_1y.empty:
                sim = simulate_sip(nav_df, start_1y.to_pydatetime(), end_date.to_pydatetime())
                if sim[0] is not None:
                    invested, value, dates, cashflows = sim
                    # xirr expects cashflows and datetime dates
                    try:
                        irr = xirr(cashflows, dates)
                        sip_results["1Y_SIP_XIRR_%"] = round(irr * 100, 2)
                    except Exception:
                        sip_results["1Y_SIP_XIRR_%"] = None
        except Exception as e:
            print(f"⚠️ Error computing SIP XIRR for {amfi_code}: {e}")

        res = {"Scheme Code": amfi_code, "Scheme Name": scheme_name}
        res.update(per_results)
        res.update(sip_results)
        all_results.append(res)

    if all_results:
        df_out = pd.DataFrame(all_results)
        df_out.to_csv(OUTPUT_FILE, index=False)
        print(f"\n✅ Results saved to {OUTPUT_FILE}\n")
        print(df_out.to_string(index=False))
    else:
        print("⚠️ No valid results to save.")


if __name__ == "__main__":
    main()
