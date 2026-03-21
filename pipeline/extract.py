"""
extract.py
Pulls raw market data from FRED (CSV endpoint) and BLS (public API v1).
No API keys required for either source.

Sources:
  FRED  — Federal Reserve Bank of St. Louis Economic Data
  BLS   — Bureau of Labor Statistics public API
"""

import requests
import pandas as pd
from io import StringIO
import logging

log = logging.getLogger(__name__)

# ── FRED SERIES ────────────────────────────────────────────────────────────────
# All available via public CSV endpoint — no registration required.

FRED_SERIES = {
    # Residential signals
    "housing_starts":                       "HOUST",
    "mortgage_rate_30yr":                   "MORTGAGE30US",
    "home_price_index":                     "CSUSHPISA",
    "construction_spending":                "TTLCONS",
    "building_materials_ppi":               "PCU327327",
    # Commercial / hospitality signals
    # Onyx dealers serve hotels, senior living, fitness, multifamily
    "nonresidential_construction_spending": "TLNRESCONS",
}

def fetch_fred_series(series_id: str) -> pd.DataFrame:
    """
    Fetch a single FRED series via public CSV endpoint.
    Returns DataFrame with columns: date, value, series_id
    """
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()

    df = pd.read_csv(StringIO(resp.text))
    df.columns = ["date", "value"]
    df["date"]     = pd.to_datetime(df["date"])
    df["value"]    = pd.to_numeric(df["value"], errors="coerce")
    df["series_id"] = series_id
    df = df.dropna(subset=["value"]).sort_values("date").reset_index(drop=True)
    return df


def fetch_all_fred(start_year: int = 2019) -> pd.DataFrame:
    """
    Fetch all configured FRED series.
    Returns combined DataFrame with columns: date, value, series_id, series_name
    """
    frames = []
    for name, sid in FRED_SERIES.items():
        try:
            df = fetch_fred_series(sid)
            df = df[df["date"].dt.year >= start_year].copy()
            df["series_name"] = name
            df["source"] = "FRED"
            frames.append(df)
            log.info(f"  FRED {sid:14s} ({name}): {len(df):3d} rows | "
                     f"latest {df['date'].max().date()} = {df['value'].iloc[-1]:.2f}")
        except Exception as e:
            log.error(f"  FRED {sid} FAILED: {e}")

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ── BLS SERIES ─────────────────────────────────────────────────────────────────
# Public API v1 — no registration or key required.
# Rate limit: 25 queries per 10 seconds (unauthenticated).

BLS_SERIES = {
    # Residential signals
    "CES2023610001": "residential_construction_employment",    # Thousands of jobs
    "CES2000000001": "total_construction_employment",          # Thousands of jobs
    "CES2023800001": "specialty_trade_employment",             # Plumbing, HVAC, tile — thousands
    # Commercial / hospitality signals
    "CES2023620001": "nonresidential_construction_employment", # Nonres building construction, thousands
    "CES7072100001": "hotel_motel_employment",                 # Hotels & motels — active renovation proxy
}

def fetch_bls_series(series_id: str, start_year: int = 2019) -> pd.DataFrame:
    """
    Fetch a single BLS series via public API v1.
    Returns DataFrame with columns: date, value, series_id
    """
    url  = f"https://api.bls.gov/publicAPI/v1/timeseries/data/{series_id}"
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    payload = resp.json()

    if payload.get("status") != "REQUEST_SUCCEEDED":
        raise ValueError(f"BLS error for {series_id}: {payload.get('message', 'unknown error')}")

    raw_rows = payload["Results"]["series"][0]["data"]
    records  = []
    for r in raw_rows:
        if r["period"] == "M13":             # skip annual average entries
            continue
        year  = int(r["year"])
        month = int(r["period"].replace("M", ""))
        if year < start_year:
            continue
        records.append({
            "date":      pd.Timestamp(year=year, month=month, day=1),
            "value":     float(r["value"]),
            "series_id": series_id,
        })

    df = pd.DataFrame(records).sort_values("date").reset_index(drop=True)
    return df


def fetch_all_bls(start_year: int = 2019) -> pd.DataFrame:
    """
    Fetch all configured BLS series.
    Returns combined DataFrame with columns: date, value, series_id, series_name, source
    """
    frames = []
    for sid, name in BLS_SERIES.items():
        try:
            df = fetch_bls_series(sid, start_year=start_year)
            df["series_name"] = name
            df["source"] = "BLS"
            frames.append(df)
            log.info(f"  BLS  {sid:14s} ({name}): {len(df):3d} rows | "
                     f"latest {df['date'].max().date()} = {df['value'].iloc[-1]:.1f}k")
        except Exception as e:
            log.error(f"  BLS {sid} FAILED: {e}")

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
