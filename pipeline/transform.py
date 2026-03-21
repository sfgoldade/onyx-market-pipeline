"""
transform.py
Cleans raw extracts and derives the analytics-ready metrics
used by the Onyx Collection market dashboard.

Key outputs:
  market_context      — monthly snapshot of macro indicators
  housing_health      — composite housing market health index (0–100)
  remodel_demand      — estimated remodeling demand index
  onyx_market_sizing  — Onyx TAM/SAM/SOM updated from live data
  dashboard_payload   — flattened JSON ready for the React dashboard
"""

import pandas as pd
import numpy as np
from datetime import datetime
import logging

log = logging.getLogger(__name__)

# ── CONSTANTS ──────────────────────────────────────────────────────────────────
# These anchors come from published market research (sourced in README).
# They define the relationship between macro indicators and Onyx's addressable market.

SHOWER_ENCLOSURE_MKT_2024_B   = 4.25   # $4.25B — Research & Markets / GlobalNewswire Jan 2024
SHOWER_ENCLOSURE_CAGR          = 0.052  # 5.2% CAGR 2022–2028
PLUMBING_FIXTURES_MKT_2024_B   = 29.2  # $29.2B — Grand View Research 2024
PLUMBING_FIXTURES_CAGR         = 0.077  # 7.7% CAGR 2025–2030
KB_REMODEL_SPEND_2024_B        = 66.7  # $66.7B — NKBA 2024
ONYX_REV_EST_MID_M             = 63.0  # $63M midpoint — Buzzfile/Manta average
ONYX_REV_EST_LO_M              = 50.0
ONYX_REV_EST_HI_M              = 75.0
HOUSING_STARTS_LONG_RUN_AVG    = 1500  # thousands/yr — pre-pandemic baseline
MORTGAGE_RATE_NEUTRAL           = 5.5  # % — NKBA threshold for remodel demand unlock

MORTGAGE_RATE_NEUTRAL           = 5.5  # % — NKBA threshold for remodel demand unlock

# ── Commercial channel baselines (2019 annual averages, BLS/FRED historical) ──
# Source: BLS CES historical tables; FRED TLNRESCONS 2019 annual average
NR_SPEND_2019_AVG_M            = 836_800   # millions/month — FRED TLNRESCONS 2019 avg
HOTEL_EMP_2019_AVG_K           = 1_630.0   # thousands — BLS CES7072100001 2019 avg
NONRES_CONST_EMP_2019_AVG_K    = 857.0     # thousands — BLS CES2023620001 2019 avg
# Commercial addressable market for Onyx
# Source: shower enclosure market is ~18% commercial (hospitality + healthcare + multifamily)
# Grand View Research 2024 commercial segment estimate
COMMERCIAL_SHARE_OF_SAM        = 0.18      # 18% of shower enclosure SAM is commercial
ONYX_EST_COMMERCIAL_MIX        = 0.06      # Onyx est. ~6% commercial (underpenetrated vs. market)


def wide_pivot(raw: pd.DataFrame) -> pd.DataFrame:
    """Pivot long raw DataFrame into wide monthly table indexed by date."""
    raw = raw.copy()
    # Normalize to month-start
    raw["date"] = raw["date"].dt.to_period("M").dt.to_timestamp()
    pivot = (
        raw
        .groupby(["date", "series_name"])["value"]
        .last()
        .unstack("series_name")
        .sort_index()
    )
    return pivot


def compute_market_context(wide: pd.DataFrame) -> pd.DataFrame:
    """
    Build the core monthly market context table.
    All columns are either directly from sources or clearly derived.
    """
    df = wide.copy()
    df.index.name = "date"
    df = df.reset_index()

    # ── Starts health (0–100 index vs. long-run average)
    if "housing_starts" in df.columns:
        df["starts_health_idx"] = (
            (df["housing_starts"] / HOUSING_STARTS_LONG_RUN_AVG * 100)
            .clip(0, 120)
            .round(1)
        )

    # ── Mortgage affordability pressure (inverted: lower rate = higher score)
    if "mortgage_rate_30yr" in df.columns:
        df["mortgage_pressure"] = (
            ((df["mortgage_rate_30yr"] - 3) / (8 - 3) * 100)
            .clip(0, 100)
            .round(1)
        )
        df["mortgage_affordability_score"] = (100 - df["mortgage_pressure"]).round(1)

    # ── Building materials cost pressure (YoY % change in PPI)
    if "building_materials_ppi" in df.columns:
        df["materials_ppi_yoy_pct"] = (
            df["building_materials_ppi"].pct_change(12) * 100
        ).round(2)

    # ── Construction employment momentum (3-month change %)
    if "residential_construction_employment" in df.columns:
        df["res_construction_emp_3m_chg"] = (
            df["residential_construction_employment"].pct_change(3) * 100
        ).round(2)

    # ── Home price appreciation (YoY)
    if "home_price_index" in df.columns:
        df["home_price_yoy_pct"] = (
            df["home_price_index"].pct_change(12) * 100
        ).round(2)

    # ── Composite housing health index (0–100)
    # Weighted average of: starts health (40%), affordability (35%), employment (25%)
    components = []
    weights    = []

    if "starts_health_idx" in df.columns:
        components.append(df["starts_health_idx"].fillna(50))
        weights.append(0.40)
    if "mortgage_affordability_score" in df.columns:
        components.append(df["mortgage_affordability_score"].fillna(50))
        weights.append(0.35)
    if "res_construction_emp_3m_chg" in df.columns:
        # Normalize employment momentum to 0–100 scale
        emp_norm = (df["res_construction_emp_3m_chg"].fillna(0) + 3) / 6 * 100
        components.append(emp_norm.clip(0, 100))
        weights.append(0.25)

    if components:
        total_weight = sum(weights)
        df["housing_health_index"] = sum(
            c * (w / total_weight) for c, w in zip(components, weights)
        ).round(1)

    # ── Remodel demand signal
    # Logic: remodel demand is counter-cyclical to new construction
    # High mortgage rates suppress moves → owners remodel in place
    if "mortgage_rate_30yr" in df.columns and "housing_starts" in df.columns:
        # Rate lock-in effect: higher rates above 5.5% increase remodel intent
        rate_lock_boost = (
            (df["mortgage_rate_30yr"] - MORTGAGE_RATE_NEUTRAL)
            .clip(0, 3) / 3 * 20        # up to +20 index points
        )
        starts_drag = (
            (HOUSING_STARTS_LONG_RUN_AVG - df["housing_starts"])
            .clip(0, 500) / 500 * 10    # up to +10 points from slow starts
        )
        df["remodel_demand_index"] = (70 + rate_lock_boost + starts_drag).round(1).clip(0, 100)


    # ── Commercial opportunity index (0–100) ─────────────────────────────────
    # Tracks the commercial/hospitality construction channel — independent of R&R.
    # Hotels, senior living, fitness, multifamily are Onyx's underpenetrated segments.
    # Index = nonresidential spending health (60%) + hotel employment health (40%)
    # Both normalized to 2019 baseline = 100. Clipped to 0–100 scale.
    if "nonresidential_construction_spending" in df.columns:
        df["nr_spend_vs_baseline"] = (
            df["nonresidential_construction_spending"] / NR_SPEND_2019_AVG_M * 100
        ).clip(0, 140).round(1)

    if "hotel_motel_employment" in df.columns:
        df["hotel_emp_vs_baseline"] = (
            df["hotel_motel_employment"] / HOTEL_EMP_2019_AVG_K * 100
        ).clip(0, 130).round(1)

    if "nonresidential_construction_employment" in df.columns:
        df["nonres_const_emp_vs_baseline"] = (
            df["nonresidential_construction_employment"] / NONRES_CONST_EMP_2019_AVG_K * 100
        ).clip(0, 130).round(1)

    # Composite commercial index
    comm_components = []
    comm_weights    = []
    if "nr_spend_vs_baseline" in df.columns:
        comm_components.append(df["nr_spend_vs_baseline"].fillna(50).clip(0, 100))
        comm_weights.append(0.60)
    if "hotel_emp_vs_baseline" in df.columns:
        comm_components.append(df["hotel_emp_vs_baseline"].fillna(50).clip(0, 100))
        comm_weights.append(0.40)

    if comm_components:
        total_cw = sum(comm_weights)
        df["commercial_opportunity_index"] = sum(
            c * (w / total_cw) for c, w in zip(comm_components, comm_weights)
        ).round(1)

    # YoY change in nonresidential spending (cost pressure + pipeline signal)
    if "nonresidential_construction_spending" in df.columns:
        df["nr_spend_yoy_pct"] = (
            df["nonresidential_construction_spending"].pct_change(12) * 100
        ).round(2)

    return df


def compute_market_sizing(context: pd.DataFrame) -> pd.DataFrame:
    """
    Project Onyx TAM/SAM/SOM forward using live housing starts as the
    demand scaler, anchored to the 2024 published market research figures.
    """
    latest = context.dropna(subset=["housing_starts"]).iloc[-1]
    latest_date  = latest["date"]
    starts_scalar = latest["housing_starts"] / HOUSING_STARTS_LONG_RUN_AVG

    # Years since 2024 base
    years_since_base = (latest_date.year - 2024) + (latest_date.month - 1) / 12

    # Market size projections (CAGR from research, scaled by live starts)
    tam_current = PLUMBING_FIXTURES_MKT_2024_B  * (1 + PLUMBING_FIXTURES_CAGR) ** years_since_base
    sam_current = SHOWER_ENCLOSURE_MKT_2024_B   * (1 + SHOWER_ENCLOSURE_CAGR)  ** years_since_base

    # Onyx market share (midpoint estimate)
    onyx_share_pct    = ONYX_REV_EST_MID_M / (sam_current * 1000) * 100

    # SOM: Onyx's realistic 3-year target at 1.8% SAM share
    som_target_m = sam_current * 1000 * 0.018

    def _safe(val, default=0.0):
        """Return float or default if NaN/None."""
        try:
            f = float(val)
            return default if f != f else f  # NaN check
        except (TypeError, ValueError):
            return default

    # Pull commercial metrics from latest context — prefer most recent non-null value
    def _latest_val(col):
        series = context[col].dropna() if col in context.columns else pd.Series(dtype=float)
        return float(series.iloc[-1]) if len(series) > 0 else 0.0

    records = [{
        "as_of_date":                   latest_date,
        "housing_starts_latest":        float(latest["housing_starts"]),
        "starts_scalar":                round(float(starts_scalar), 3),
        "tam_b":                        round(tam_current, 2),
        "sam_b":                        round(sam_current, 2),
        "som_m":                        round(som_target_m, 1),
        "onyx_rev_est_mid_m":           ONYX_REV_EST_MID_M,
        "onyx_rev_est_lo_m":            ONYX_REV_EST_LO_M,
        "onyx_rev_est_hi_m":            ONYX_REV_EST_HI_M,
        "onyx_share_of_sam_pct":        round(onyx_share_pct, 3),
        "mortgage_rate_latest":         round(_safe(latest.get("mortgage_rate_30yr")), 2),
        "remodel_demand_index":         round(_safe(latest.get("remodel_demand_index")), 1),
        "housing_health_index":         round(_safe(latest.get("housing_health_index")), 1),
        "commercial_opportunity_index": round(_latest_val("commercial_opportunity_index"), 1),
        "nr_spend_vs_baseline":         round(_latest_val("nr_spend_vs_baseline"), 1),
        "hotel_emp_vs_baseline":        round(_latest_val("hotel_emp_vs_baseline"), 1),
        "data_sources":                 "FRED (HOUST, MORTGAGE30US, CSUSHPISA, TLNRESCONS); "
                                        "BLS (CES2023610001, CES2023620001, CES7072100001); "
                                        "Research & Markets Jan 2024; Grand View Research 2024; "
                                        "NKBA 2024; Buzzfile/Manta (Onyx est.)",
    }]
    return pd.DataFrame(records)


def build_dashboard_payload(context: pd.DataFrame, sizing: pd.DataFrame) -> dict:
    """
    Build the JSON payload consumed by the React dashboard's live data layer.
    Keyed for direct drop-in replacement of hardcoded arrays.
    """
    # Last 36 months of context for charts
    ctx = context.dropna(subset=["housing_starts"]).tail(36).copy()

    def safe(val):
        if pd.isna(val): return None
        if hasattr(val, 'item'): return val.item()
        return val

    housing_trend = [
        {
            "date":           r["date"].strftime("%Y-%m"),
            "housing_starts": safe(r.get("housing_starts")),
            "mortgage_rate":  safe(r.get("mortgage_rate_30yr")),
            "health_index":   safe(r.get("housing_health_index")),
            "remodel_index":  safe(r.get("remodel_demand_index")),
            "home_price_yoy":        safe(r.get("home_price_yoy_pct")),
            "commercial_index":      safe(r.get("commercial_opportunity_index")),
            "nr_spend_baseline_pct": safe(r.get("nr_spend_vs_baseline")),
            "hotel_emp_baseline_pct":safe(r.get("hotel_emp_vs_baseline")),
        }
        for _, r in ctx.iterrows()
    ]

    sz = sizing.iloc[0]

    payload = {
        "generated_at":   datetime.utcnow().isoformat() + "Z",
        "data_as_of":     str(sz["as_of_date"])[:10],
        "market_sizing": {
            "tam_b":               safe(sz["tam_b"]),
            "sam_b":               safe(sz["sam_b"]),
            "som_m":               safe(sz["som_m"]),
            "onyx_rev_est_mid_m":  safe(sz["onyx_rev_est_mid_m"]),
            "onyx_rev_est_lo_m":   safe(sz["onyx_rev_est_lo_m"]),
            "onyx_rev_est_hi_m":   safe(sz["onyx_rev_est_hi_m"]),
            "onyx_share_pct":      safe(sz["onyx_share_of_sam_pct"]),
        },
        "live_indicators": {
            "housing_starts":             safe(sz["housing_starts_latest"]),
            "mortgage_rate":              safe(sz["mortgage_rate_latest"]),
            "housing_health_index":       safe(sz["housing_health_index"]),
            "remodel_demand_index":       safe(sz["remodel_demand_index"]),
            "commercial_opportunity_index": safe(sz["commercial_opportunity_index"]),
            "nr_spend_vs_baseline":       safe(sz["nr_spend_vs_baseline"]),
            "hotel_emp_vs_baseline":      safe(sz["hotel_emp_vs_baseline"]),
        },
        "housing_trend": housing_trend,
        "sources": {
            "FRED_HOUST":       "US Housing Starts (SA, thousands) — Federal Reserve St. Louis",
            "FRED_MORTGAGE30US":"30-Year Fixed Mortgage Rate — Freddie Mac / FRED",
            "FRED_CSUSHPISA":   "Case-Shiller US National Home Price Index — S&P/FRED",
            "FRED_TTLCONS":     "Total Construction Spending — US Census Bureau / FRED",
            "FRED_PCU327327":   "PPI Nonmetallic Mineral Products — BLS / FRED",
            "BLS_CES2023610001":"Residential Building Construction Employment — BLS",
            "BLS_CES2000000001":"Total Construction Employment — BLS",
            "BLS_CES2023800001":"Specialty Trade Contractors Employment — BLS",
            "NKBA_2024":        "NKBA 2024 Kitchen & Bath Market Outlook",
            "RESEARCH_MARKETS": "US Shower Enclosures Market 2023–2028, Jan 2024",
            "GRAND_VIEW":       "NA Plumbing Fixtures Market, Grand View Research 2024",
            "ONYX_EST":          "Onyx revenue estimated from Buzzfile/Manta public records",
            "FRED_TLNRESCONS":   "Total Nonresidential Construction Spending — US Census Bureau / FRED",
            "BLS_CES2023620001": "Nonresidential Building Construction Employment — BLS",
            "BLS_CES7072100001": "Hotels & Motels Employment — BLS (renovation/expansion proxy)",
        }
    }
    return payload


def run_transforms(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Main transform entry point. Returns (market_context, market_sizing, dashboard_payload).
    """
    log.info("Pivoting raw data to wide format...")
    wide = wide_pivot(raw_df)
    log.info(f"  Wide table: {len(wide)} months × {len(wide.columns)} series")

    log.info("Computing market context metrics...")
    context = compute_market_context(wide)
    log.info(f"  Context table: {len(context)} rows, "
             f"{context['date'].min().date()} → {context['date'].max().date()}")

    log.info("Computing market sizing...")
    sizing = compute_market_sizing(context)
    sz = sizing.iloc[0]
    log.info(f"  TAM ${sz['tam_b']:.2f}B | SAM ${sz['sam_b']:.2f}B | "
             f"Housing health {sz['housing_health_index']:.0f}/100 | "
             f"Remodel demand {sz['remodel_demand_index']:.0f}/100 | "
             f"Commercial index {sz['commercial_opportunity_index']:.0f}/100")

    log.info("Building dashboard payload...")
    payload = build_dashboard_payload(context, sizing)
    log.info(f"  Payload: {len(payload['housing_trend'])} months of trend data")

    return context, sizing, payload
