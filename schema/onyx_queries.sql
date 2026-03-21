-- ============================================================
-- onyx_queries.sql
-- Analytics query library for the Onyx market intelligence DB.
-- These demonstrate the types of questions the pipeline enables.
-- ============================================================


-- ── 1. LATEST MARKET SNAPSHOT ───────────────────────────────
-- The single most important read: where are we right now?

SELECT
    mc.date                                         AS month,
    mc.housing_starts                               AS housing_starts_k,
    mc.mortgage_rate_30yr                           AS mortgage_rate_pct,
    mc.housing_health_index                         AS housing_health_0_100,
    mc.remodel_demand_index                         AS remodel_demand_0_100,
    mc.home_price_yoy_pct                           AS home_price_yoy,
    mc.materials_ppi_yoy_pct                        AS materials_cost_pressure_yoy,
    mss.tam_b                                       AS tam_billions,
    mss.sam_b                                       AS sam_billions,
    mss.onyx_rev_est_mid_m                          AS onyx_rev_est_m,
    mss.onyx_share_of_sam_pct                       AS onyx_share_pct
FROM   market_context mc
CROSS  JOIN (SELECT * FROM market_sizing_snapshots ORDER BY created_at DESC LIMIT 1) mss
ORDER  BY mc.date DESC
LIMIT  1;


-- ── 2. HOUSING HEALTH TREND (last 24 months) ────────────────
-- Tracks whether the macro environment is getting better or worse for Onyx.

SELECT
    date,
    housing_starts,
    mortgage_rate_30yr,
    ROUND(housing_health_index, 1)                  AS health_idx,
    ROUND(remodel_demand_index, 1)                  AS remodel_idx,
    CASE
        WHEN housing_health_index >= 75 THEN 'Strong'
        WHEN housing_health_index >= 55 THEN 'Moderate'
        ELSE 'Weak'
    END                                             AS macro_environment
FROM   market_context
WHERE  date >= DATE('now', '-24 months')
ORDER  BY date;


-- ── 3. RATE LOCK-IN SIGNAL ──────────────────────────────────
-- Identifies months where remodel demand is elevated relative to starts health.
-- This is the core counter-cyclical thesis: rates suppress moves, boost remodeling.

SELECT
    date,
    mortgage_rate_30yr,
    ROUND(housing_health_index, 1)                  AS housing_health,
    ROUND(remodel_demand_index, 1)                  AS remodel_demand,
    ROUND(remodel_demand_index - housing_health_index, 1) AS remodel_vs_housing_spread,
    CASE
        WHEN remodel_demand_index - housing_health_index > 10
            THEN 'STRONG REMODEL TAILWIND'
        WHEN remodel_demand_index - housing_health_index > 0
            THEN 'Moderate tailwind'
        ELSE 'Neutral / headwind'
    END                                             AS signal
FROM   market_context
WHERE  date >= DATE('now', '-24 months')
ORDER  BY date DESC;


-- ── 4. BUILDING MATERIALS COST PRESSURE ─────────────────────
-- PPI trend for nonmetallic minerals (stone, ceramic) — Onyx's input cost proxy.

SELECT
    date,
    building_materials_ppi,
    ROUND(materials_ppi_yoy_pct, 2)                AS ppi_yoy_pct,
    CASE
        WHEN materials_ppi_yoy_pct >  5 THEN 'HIGH PRESSURE — margin risk'
        WHEN materials_ppi_yoy_pct >  2 THEN 'Elevated — monitor'
        WHEN materials_ppi_yoy_pct > -1 THEN 'Stable'
        ELSE 'Deflationary — margin tailwind'
    END                                             AS cost_signal
FROM   market_context
WHERE  building_materials_ppi IS NOT NULL
  AND  date >= DATE('now', '-18 months')
ORDER  BY date DESC;


-- ── 5. MARKET SHARE TRAJECTORY ──────────────────────────────
-- How is Onyx's estimated share moving over time?
-- Requires market_sizing_snapshots to have multiple entries (builds over time).

SELECT
    snapshot_date,
    ROUND(sam_b, 2)                                 AS sam_b,
    onyx_rev_est_mid_m,
    ROUND(onyx_share_of_sam_pct, 3)                AS share_pct,
    ROUND(onyx_share_of_sam_pct
          - LAG(onyx_share_of_sam_pct) OVER (ORDER BY snapshot_date), 3)
                                                    AS share_delta_pp,
    housing_health_index,
    remodel_demand_index
FROM   market_sizing_snapshots
ORDER  BY snapshot_date;


-- ── 6. PIPELINE HEALTH MONITOR ──────────────────────────────
-- Audit log of all runs — useful for debugging and demonstrating reliability.

SELECT
    run_at,
    status,
    rows_extracted,
    rows_loaded,
    fred_series     || ' FRED + ' || bls_series || ' BLS' AS series_pulled,
    latest_data_date,
    ROUND(duration_sec, 1)                          AS duration_s,
    git_sha,
    COALESCE(error_message, '—')                    AS error
FROM   pipeline_runs
ORDER  BY run_at DESC
LIMIT  20;


-- ── 7. CORRELATION: STARTS vs. SPECIALTY TRADE EMPLOYMENT ───
-- Specialty trade (plumbers, tile setters) is a proxy for installed base demand.
-- High specialty employment at low starts = remodel activity is carrying the sector.

SELECT
    mc.date,
    mc.housing_starts,
    mc.specialty_trade_employment                   AS specialty_trade_k,
    mc.res_construction_employment                  AS res_construction_k,
    ROUND(
        CAST(mc.specialty_trade_employment AS REAL)
        / NULLIF(mc.res_construction_employment, 0)
    , 3)                                            AS specialty_to_residential_ratio,
    mc.remodel_demand_index
FROM   market_context mc
WHERE  mc.specialty_trade_employment IS NOT NULL
  AND  mc.date >= DATE('now', '-24 months')
ORDER  BY mc.date DESC;


-- ── 8. WHAT WOULD THE INTERNAL DEALER QUERY LOOK LIKE? ──────
-- This demonstrates the cross-layer join intent —
-- combining pipeline market data with internal ERP dealer data.
-- NOTE: _model_dealers and _model_orders are schema stubs (no internal data).

/*
SELECT
    d.dealer_name,
    d.region,
    d.tier,
    SUM(o.revenue)                                  AS ttm_revenue,
    AVG(o.gross_margin)                             AS avg_margin,
    SUM(CASE WHEN o.product_line = 'Accessories'
             THEN o.revenue ELSE 0 END)
    / NULLIF(SUM(o.revenue), 0) * 100               AS accessories_pct,
    mc.remodel_demand_index                         AS regional_demand_signal,
    mc.housing_health_index                         AS regional_housing_health
FROM   _model_dealers d
JOIN   _model_orders o
    ON  o.dealer_id = d.dealer_id
    AND o.order_date >= DATE('now', '-12 months')
JOIN   market_context mc
    ON  mc.date = DATE('now', 'start of month', '-1 month')  -- latest macro
WHERE  d.active = 1
GROUP  BY d.dealer_id, d.dealer_name, d.region, d.tier,
          mc.remodel_demand_index, mc.housing_health_index
ORDER  BY ttm_revenue DESC;
*/

-- ── 8. DUAL CHANNEL SIGNAL — RESIDENTIAL vs. COMMERCIAL ────────────────────
-- The core strategic finding: are both channels favorable simultaneously?
-- When both remodel_demand_index > 70 AND commercial_opportunity_index > 70,
-- Onyx has concurrent tailwinds across its entire addressable market.

SELECT
    mc.date,
    ROUND(mc.remodel_demand_index, 1)         AS remodel_demand,
    ROUND(mc.housing_health_index, 1)         AS housing_health,
    ROUND(mc.commercial_opportunity_index, 1) AS commercial_opp,
    ROUND(mc.remodel_demand_index
          - mc.housing_health_index, 1)        AS residential_spread_pp,
    ROUND(mc.nr_spend_vs_baseline, 1)         AS nr_spend_pct_of_baseline,
    ROUND(mc.hotel_emp_vs_baseline, 1)        AS hotel_emp_pct_of_baseline,
    CASE
        WHEN mc.remodel_demand_index > 70
         AND mc.commercial_opportunity_index > 70
            THEN 'DUAL GREEN LIGHT — both channels favorable'
        WHEN mc.remodel_demand_index > 70
            THEN 'R&R tailwind only'
        WHEN mc.commercial_opportunity_index > 70
            THEN 'Commercial tailwind only'
        ELSE 'Neutral / mixed'
    END                                        AS channel_signal
FROM   market_context mc
WHERE  mc.remodel_demand_index IS NOT NULL
  AND  mc.commercial_opportunity_index IS NOT NULL
  AND  mc.date >= DATE('now', '-18 months')
ORDER  BY mc.date DESC;


-- ── 9. COMMERCIAL GAP ANALYSIS ─────────────────────────────────────────────
-- Quantifies the commercial channel opportunity.
-- Market share: commercial is ~18% of shower enclosure SAM (Research & Markets).
-- Onyx estimated commercial mix: ~6%.
-- Gap = (18% - 6%) × current SAM = forgone commercial revenue opportunity.

SELECT
    mss.snapshot_date,
    ROUND(mss.sam_b, 2)                              AS sam_b,
    0.18                                              AS mkt_commercial_share,
    ROUND(mss.sam_b * 1000 * 0.18, 1)               AS mkt_commercial_sam_m,
    0.06                                              AS onyx_est_commercial_mix,
    ROUND(mss.onyx_rev_est_mid_m * 0.06, 1)         AS onyx_est_commercial_rev_m,
    ROUND(mss.sam_b * 1000 * 0.18
          - mss.onyx_rev_est_mid_m * 0.06, 1)        AS commercial_gap_m,
    mss.commercial_opportunity_index                  AS commercial_index,
    mss.hotel_emp_vs_baseline                         AS hotel_index
FROM   market_sizing_snapshots mss
ORDER  BY mss.snapshot_date DESC
LIMIT  4;
