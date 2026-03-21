"""
load.py
Writes transformed data to:
  1. SQLite database  — queryable history for trend analysis
  2. JSON export      — consumed by React dashboard (live data layer)
  3. Run log table    — pipeline execution metadata for monitoring

Schema mirrors what the full internal Onyx analytics stack would use,
demonstrating data modeling intent even without access to internal data.
"""

import sqlite3
import json
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging

log = logging.getLogger(__name__)

DB_PATH   = Path(__file__).parent.parent / "data" / "onyx_market.db"
JSON_PATH = Path(__file__).parent.parent / "data" / "dashboard_payload.json"


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_schema(conn: sqlite3.Connection) -> None:
    """
    Create all tables if they don't exist.

    Tables here model both what we CAN populate (market data) and what the
    INTERNAL Onyx analytics stack WOULD contain — demonstrating full-stack
    data modeling intent for the interview panel.
    """
    conn.executescript("""
        -- ── POPULATED BY THIS PIPELINE ────────────────────────────────────────

        CREATE TABLE IF NOT EXISTS raw_series (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        DATE    NOT NULL,
            series_id   TEXT    NOT NULL,
            series_name TEXT    NOT NULL,
            value       REAL    NOT NULL,
            source      TEXT    NOT NULL,          -- 'FRED' or 'BLS'
            loaded_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, series_id)
        );

        CREATE TABLE IF NOT EXISTS market_context (
            id                                  INTEGER PRIMARY KEY AUTOINCREMENT,
            date                                DATE    NOT NULL UNIQUE,
            housing_starts                      REAL,
            mortgage_rate_30yr                  REAL,
            home_price_index                    REAL,
            construction_spending               REAL,
            building_materials_ppi              REAL,
            res_construction_employment         REAL,
            total_construction_employment       REAL,
            specialty_trade_employment          REAL,
            nonresidential_construction_spending    REAL,
            nonresidential_construction_employment  REAL,
            hotel_motel_employment              REAL,
            starts_health_idx                   REAL,
            mortgage_affordability_score        REAL,
            housing_health_index                REAL,
            remodel_demand_index                REAL,
            materials_ppi_yoy_pct               REAL,
            home_price_yoy_pct                  REAL,
            nr_spend_vs_baseline                REAL,
            hotel_emp_vs_baseline               REAL,
            nonres_const_emp_vs_baseline        REAL,
            commercial_opportunity_index        REAL,
            nr_spend_yoy_pct                    REAL,
            loaded_at                           DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS market_sizing_snapshots (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date           DATE    NOT NULL,
            tam_b                   REAL,   -- Total addressable market, $B
            sam_b                   REAL,   -- Serviceable addressable market, $B
            som_m                   REAL,   -- Serviceable obtainable market, $M
            onyx_rev_est_mid_m      REAL,   -- Onyx revenue midpoint estimate
            onyx_rev_est_lo_m       REAL,
            onyx_rev_est_hi_m       REAL,
            onyx_share_of_sam_pct   REAL,   -- Onyx estimated share of SAM
            housing_starts_latest   REAL,
            mortgage_rate_latest    REAL,
            housing_health_index    REAL,
            remodel_demand_index    REAL,
            data_sources            TEXT,
            created_at              DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
            status          TEXT    NOT NULL,   -- 'success' | 'partial' | 'failed'
            rows_extracted  INTEGER,
            rows_loaded     INTEGER,
            fred_series     INTEGER,
            bls_series      INTEGER,
            latest_data_date DATE,
            duration_sec    REAL,
            error_message   TEXT,
            git_sha         TEXT
        );

        -- ── INTERNAL MODEL — WHAT THE FULL ONYX STACK WOULD LOOK LIKE ────────
        -- These tables are NOT populated by this pipeline (no internal data).
        -- They demonstrate the target state data architecture for interview context.

        CREATE TABLE IF NOT EXISTS _model_dealers (
            dealer_id       TEXT    PRIMARY KEY,   -- e.g., 'DLR-0001'
            dealer_name     TEXT    NOT NULL,
            region          TEXT,                  -- Southeast, Midwest, West Coast, etc.
            state           TEXT,
            tier            TEXT,                  -- Gold | Silver | Bronze
            active          INTEGER DEFAULT 1,
            signup_date     DATE,
            exclusivity     INTEGER DEFAULT 0,     -- 1 if exclusivity agreement signed
            primary_rep     TEXT,                  -- sales rep ID
            created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS _model_orders (
            order_id        TEXT    PRIMARY KEY,
            dealer_id       TEXT    REFERENCES _model_dealers(dealer_id),
            order_date      DATE    NOT NULL,
            ship_date       DATE,
            product_line    TEXT,                  -- Shower Bases | Wall Panels | Doors | Vanity | Accessories
            product_sku     TEXT,
            custom_flag     INTEGER DEFAULT 0,     -- 1 if custom/made-to-order
            units           INTEGER,
            unit_price      REAL,
            revenue         REAL,
            cogs            REAL,
            gross_margin    REAL,                  -- computed: (revenue - cogs) / revenue
            region          TEXT,
            state           TEXT,
            lead_time_days  INTEGER,               -- order_date → ship_date
            on_time         INTEGER,               -- 1 if shipped by promised date
            created_at      DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS _model_dealer_metrics_monthly (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            dealer_id       TEXT    REFERENCES _model_dealers(dealer_id),
            month           DATE    NOT NULL,
            orders          INTEGER,
            revenue         REAL,
            units           INTEGER,
            avg_order_value REAL,
            product_lines   INTEGER,               -- breadth of ordering
            accessories_rev REAL,                  -- accessories attach revenue
            accessories_pct REAL,                  -- accessories as % of total
            yoy_growth_pct  REAL,                  -- vs same month prior year
            at_risk_flag    INTEGER DEFAULT 0,     -- 1 if declining 2+ consecutive months
            UNIQUE(dealer_id, month)
        );

        CREATE TABLE IF NOT EXISTS _model_market_share_tracker (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            quarter         TEXT    NOT NULL,      -- e.g., '2024-Q1'
            tam_b           REAL,                  -- from market_sizing_snapshots
            sam_b           REAL,
            onyx_rev_m      REAL,                  -- from internal ERP
            onyx_share_sam  REAL,                  -- onyx_rev / (sam * 1000) * 100
            yoy_share_delta REAL,                  -- vs prior year quarter
            created_at      DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        -- Indexes for query performance
        CREATE INDEX IF NOT EXISTS idx_raw_series_date        ON raw_series(date);
        CREATE INDEX IF NOT EXISTS idx_raw_series_name        ON raw_series(series_name);
        CREATE INDEX IF NOT EXISTS idx_market_context_date    ON market_context(date);
        CREATE INDEX IF NOT EXISTS idx_orders_dealer          ON _model_orders(dealer_id);
        CREATE INDEX IF NOT EXISTS idx_orders_date            ON _model_orders(order_date);
        CREATE INDEX IF NOT EXISTS idx_orders_product         ON _model_orders(product_line);
        CREATE INDEX IF NOT EXISTS idx_dealer_metrics_month   ON _model_dealer_metrics_monthly(month);
    """)
    conn.commit()
    # Add new columns to existing DBs (safe to run repeatedly)
    new_cols = [
        ("market_context", "nonresidential_construction_spending", "REAL"),
        ("market_context", "nonresidential_construction_employment", "REAL"),
        ("market_context", "hotel_motel_employment", "REAL"),
        ("market_context", "nr_spend_vs_baseline", "REAL"),
        ("market_context", "hotel_emp_vs_baseline", "REAL"),
        ("market_context", "nonres_const_emp_vs_baseline", "REAL"),
        ("market_context", "commercial_opportunity_index", "REAL"),
        ("market_context", "nr_spend_yoy_pct", "REAL"),
        ("market_sizing_snapshots", "commercial_opportunity_index", "REAL"),
        ("market_sizing_snapshots", "nr_spend_vs_baseline", "REAL"),
        ("market_sizing_snapshots", "hotel_emp_vs_baseline", "REAL"),
    ]
    for tbl, col, typ in new_cols:
        try:
            conn.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {typ}")
        except Exception:
            pass
    conn.commit()
    log.info("Schema initialized (8 tables)")


def upsert_raw_series(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """Upsert raw series rows, skip duplicates."""
    rows = 0
    for _, r in df.iterrows():
        try:
            conn.execute("""
                INSERT OR REPLACE INTO raw_series (date, series_id, series_name, value, source)
                VALUES (?, ?, ?, ?, ?)
            """, (r["date"].date().isoformat(), r["series_id"], r["series_name"],
                  float(r["value"]), r["source"]))
            rows += 1
        except Exception as e:
            log.warning(f"raw_series upsert error: {e}")
    conn.commit()
    return rows


def upsert_market_context(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """Upsert market context rows."""
    col_map = {
        # Residential raw series
        "housing_starts":                         "housing_starts",
        "mortgage_rate_30yr":                     "mortgage_rate_30yr",
        "home_price_index":                       "home_price_index",
        "construction_spending":                  "construction_spending",
        "building_materials_ppi":                 "building_materials_ppi",
        "residential_construction_employment":    "res_construction_employment",
        "total_construction_employment":          "total_construction_employment",
        "specialty_trade_employment":             "specialty_trade_employment",
        # Residential derived metrics
        "starts_health_idx":                      "starts_health_idx",
        "mortgage_affordability_score":           "mortgage_affordability_score",
        "housing_health_index":                   "housing_health_index",
        "remodel_demand_index":                   "remodel_demand_index",
        "materials_ppi_yoy_pct":                  "materials_ppi_yoy_pct",
        "home_price_yoy_pct":                     "home_price_yoy_pct",
        # Commercial raw series
        "nonresidential_construction_spending":   "nonresidential_construction_spending",
        "nonresidential_construction_employment": "nonresidential_construction_employment",
        "hotel_motel_employment":                 "hotel_motel_employment",
        # Commercial derived metrics
        "nr_spend_vs_baseline":                   "nr_spend_vs_baseline",
        "hotel_emp_vs_baseline":                  "hotel_emp_vs_baseline",
        "nonres_const_emp_vs_baseline":           "nonres_const_emp_vs_baseline",
        "commercial_opportunity_index":           "commercial_opportunity_index",
        "nr_spend_yoy_pct":                       "nr_spend_yoy_pct",
    }
    rows = 0
    for _, r in df.iterrows():
        date_str = r["date"].date().isoformat() if hasattr(r["date"], "date") else str(r["date"])[:10]
        vals = {db_col: (float(r[src_col]) if src_col in r and pd.notna(r[src_col]) else None)
                for src_col, db_col in col_map.items()}
        cols   = ["date"] + list(vals.keys())
        params = [date_str] + list(vals.values())
        placeholders = ",".join(["?"] * len(params))
        updates = ",".join(f"{c}=excluded.{c}" for c in vals.keys())
        try:
            conn.execute(f"""
                INSERT INTO market_context ({",".join(cols)}) VALUES ({placeholders})
                ON CONFLICT(date) DO UPDATE SET {updates}
            """, params)
            rows += 1
        except Exception as e:
            log.warning(f"market_context upsert error on {date_str}: {e}")
    conn.commit()
    return rows


def insert_sizing_snapshot(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    """Insert a new market sizing snapshot."""
    r = df.iloc[0]
    def _safe_float(val, default=0.0):
        try:
            return float(val) if val is not None and str(val) != 'nan' else default
        except (TypeError, ValueError):
            return default

    conn.execute("""
        INSERT INTO market_sizing_snapshots
          (snapshot_date, tam_b, sam_b, som_m, onyx_rev_est_mid_m, onyx_rev_est_lo_m,
           onyx_rev_est_hi_m, onyx_share_of_sam_pct, housing_starts_latest,
           mortgage_rate_latest, housing_health_index, remodel_demand_index,
           commercial_opportunity_index, nr_spend_vs_baseline, hotel_emp_vs_baseline,
           data_sources)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        str(r["as_of_date"])[:10],
        _safe_float(r["tam_b"]), _safe_float(r["sam_b"]), _safe_float(r["som_m"]),
        _safe_float(r["onyx_rev_est_mid_m"]), _safe_float(r["onyx_rev_est_lo_m"]),
        _safe_float(r["onyx_rev_est_hi_m"]), _safe_float(r["onyx_share_of_sam_pct"]),
        _safe_float(r["housing_starts_latest"]), _safe_float(r["mortgage_rate_latest"]),
        _safe_float(r["housing_health_index"]), _safe_float(r["remodel_demand_index"]),
        _safe_float(r.get("commercial_opportunity_index")),
        _safe_float(r.get("nr_spend_vs_baseline")),
        _safe_float(r.get("hotel_emp_vs_baseline")),
        str(r["data_sources"]),
    ))
    conn.commit()


def write_json_payload(payload: dict) -> None:
    """Write dashboard JSON payload to disk."""
    JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(JSON_PATH, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    log.info(f"JSON payload written → {JSON_PATH} "
             f"({JSON_PATH.stat().st_size / 1024:.1f} KB)")


def log_run(conn: sqlite3.Connection, status: str, rows_extracted: int,
            rows_loaded: int, fred_series: int, bls_series: int,
            latest_date: str, duration: float, error: str = None) -> None:
    """Record pipeline execution metadata."""
    git_sha = None
    try:
        import subprocess
        git_sha = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        pass

    conn.execute("""
        INSERT INTO pipeline_runs
          (status, rows_extracted, rows_loaded, fred_series, bls_series,
           latest_data_date, duration_sec, error_message, git_sha)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (status, rows_extracted, rows_loaded, fred_series, bls_series,
          latest_date, round(duration, 2), error, git_sha))
    conn.commit()


def run_load(raw_df: pd.DataFrame, context: pd.DataFrame,
             sizing: pd.DataFrame, payload: dict,
             duration: float, error: str = None) -> None:
    """Main load entry point — writes all outputs."""
    conn = get_connection()
    init_schema(conn)

    raw_rows    = upsert_raw_series(conn, raw_df)
    ctx_rows    = upsert_market_context(conn, context)
    insert_sizing_snapshot(conn, sizing)
    write_json_payload(payload)

    fred_ct = raw_df[raw_df["source"] == "FRED"]["series_id"].nunique() if "source" in raw_df.columns else 0
    bls_ct  = raw_df[raw_df["source"] == "BLS"]["series_id"].nunique()  if "source" in raw_df.columns else 0
    latest  = str(raw_df["date"].max().date()) if not raw_df.empty else "unknown"

    status = "partial" if error else "success"
    log_run(conn, status, len(raw_df), ctx_rows, fred_ct, bls_ct, latest, duration, error)

    log.info(f"Load complete: {raw_rows} raw rows | {ctx_rows} context rows | "
             f"{fred_ct} FRED + {bls_ct} BLS series | status={status}")
    conn.close()
