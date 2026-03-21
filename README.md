# Onyx Collection — Market Intelligence Pipeline

**Live ETL pipeline tracking US housing market dynamics and their implications for specialty bath surface manufacturers.**

Built as a commercial analytics portfolio piece targeting [The Onyx Collection](https://onyxcollection.com) — a privately held Kansas bath fixture manufacturer competing in a $29.2B North American market.

---

## What This Does

Pulls **real public market data** from two government sources across **9 series** on a weekly schedule, transforms it into analytics-ready metrics, and exports a JSON payload consumed by a live React dashboard.

```
FRED API  ──┐
             ├─► extract.py ──► transform.py ──► load.py ──► SQLite DB
BLS API   ──┘                                          └──► dashboard_payload.json
```

**No API keys required.** Both sources are fully public.

---

## Data Sources

| Source | Series | Description |
|--------|--------|-------------|
| FRED | `HOUST` | US Housing Starts (SA, thousands/mo) |
| FRED | `MORTGAGE30US` | 30-Year Fixed Mortgage Rate (%) |
| FRED | `CSUSHPISA` | Case-Shiller US National Home Price Index |
| FRED | `TTLCONS` | Total Construction Spending ($M) |
| FRED | `PCU327327` | PPI — Nonmetallic Mineral Products (stone, ceramic) |
| BLS | `CES2023610001` | Residential Building Construction Employment |
| BLS | `CES2000000001` | Total Construction Employment |
| BLS | `CES2023800001` | Specialty Trade Contractors Employment (plumbing, HVAC) |
| FRED | `TLNRESCONS` | Total Nonresidential Construction Spending ($M) |
| BLS | `CES2023620001` | Nonresidential Building Construction Employment |
| BLS | `CES7072100001` | Hotels & Motels Employment (renovation/expansion proxy) |

All market sizing anchors cite published research:
- **$29.2B NA Plumbing Fixtures** — Grand View Research, 2024
- **$4.25B US Shower Enclosures** — Research & Markets / GlobalNewswire, January 2024
- **$66.7B K&B Remodeling** — NKBA 2024 Kitchen & Bath Market Outlook
- **Onyx revenue ~$50–75M** — Buzzfile ($63.2M), Manta ($73M) public estimates

---

## Derived Metrics

| Metric | Logic | Why It Matters |
|--------|-------|----------------|
| `housing_health_index` (0–100) | Weighted composite: starts health (40%) + affordability (35%) + employment momentum (25%) | Single number summarizing macro tailwind/headwind |
| `remodel_demand_index` (0–100) | Rate lock-in effect + starts drag | Counter-cyclical signal — high rates suppress moves, boost remodeling |
| `onyx_share_of_sam_pct` | Onyx est. revenue ÷ live shower enclosure SAM | The headline KPI — are we gaining or losing ground? |
| `materials_ppi_yoy_pct` | YoY % change in nonmetallic mineral PPI | Tracks input cost pressure for Onyx manufacturing |
| `home_price_yoy_pct` | YoY % change in Case-Shiller HPI | Homeowner equity signal — drives willingness to remodel |
| `commercial_opportunity_index` (0–100) | Weighted composite: NR spending vs. 2019 baseline (60%) + hotel employment vs. 2019 baseline (40%) | Independent second-channel signal — hotels, senior living, fitness, multifamily |
| `nr_spend_vs_baseline` | NR construction spending as % of 2019 avg ($836.8B/mo) | Raw commercial construction health |
| `hotel_emp_vs_baseline` | Hotel/motel employment as % of 2019 avg (1,630K) | Active renovation cycle proxy — over-staffed hotels = renovation/expansion |

---

## Schema

The SQLite database models both what this pipeline **can populate** and what the full internal Onyx analytics stack **would look like** — demonstrating end-to-end data architecture intent.

```
MARKET DATA (populated)          INTERNAL ONYX (target state)
─────────────────────────        ──────────────────────────────
raw_series                       dealers          (dim)
market_context                   orders           (fact)
market_sizing_snapshots          dealer_metrics_monthly
pipeline_runs                    market_share_tracker
```

See [`schema/data_model.svg`](schema/data_model.svg) for the full entity-relationship diagram.

**Key design decision:** `market_share_tracker` is the cross-layer join table — it connects pipeline-sourced market sizing to internal ERP revenue data. This single join produces the most important executive KPI: *is Onyx gaining or losing share in its addressable market?*

---

## Project Structure

```
onyx-pipeline/
├── pipeline/
│   ├── extract.py       # FRED + BLS data extraction
│   ├── transform.py     # Cleaning, derivations, market sizing
│   ├── load.py          # SQLite write + JSON export + run logging
│   └── pipeline.py      # Orchestrator — runs the full ETL
├── data/
│   ├── onyx_market.db   # SQLite database (auto-committed by CI)
│   ├── dashboard_payload.json  # Live JSON for React dashboard
│   └── pipeline.log     # Execution history
├── schema/
│   └── data_model.svg   # Entity-relationship diagram
├── .github/
│   └── workflows/
│       └── pipeline.yml # GitHub Actions — runs every Sunday 06:00 UTC
├── requirements.txt
└── README.md
```

---

## Running Locally

```bash
# Clone and install
git clone https://github.com/stevegoldade/onyx-market-pipeline
cd onyx-market-pipeline
pip install -r requirements.txt

# Run the full pipeline
cd pipeline
python pipeline.py
```

Output files written to `data/`. Typical run time: 3–5 seconds.

---

## Automation

GitHub Actions runs the pipeline every **Sunday at 06:00 UTC** — after BLS Friday data releases propagate through the API. Updated `dashboard_payload.json` and `onyx_market.db` are committed back to the repo automatically.

Manual runs are available via the GitHub Actions UI (`workflow_dispatch`).

---

## Strategic Context

This pipeline is part of a broader commercial analytics assessment of The Onyx Collection. Key findings from the live data as of the latest run:

- **Housing health index: ~66/100** — starts recovering from 2023 lows but below long-run average
- **Remodel demand index: ~74/100** — elevated by rate lock-in effect (6%+ mortgage rates suppress moves, homeowners remodel instead)
- **Building materials PPI: +4.1% YoY** — input cost pressure to monitor for margin impact
- **Onyx estimated market share: ~1.3% of $4.7B shower enclosure SAM** — significant room vs. 1.8% 3-year target

The `remodel_demand_index` being elevated while `housing_health_index` is suppressed is the first strategic insight: **Onyx's core R&R customer is being supported by exactly the macro environment that's pressuring new construction.** This is the residential counter-cyclical thesis in a single number.

A second finding emerged from adding the commercial series: `commercial_opportunity_index` ran at **100/100** (nonresidential spending 40% above 2019 baseline, hotel employment 119/100) throughout all of 2025 — simultaneously with the elevated remodel demand signal. Both channels are green at the same time. Onyx is estimated at ~6% commercial mix against an 18% commercial share of the total shower enclosure market. That gap is the commercial channel opportunity.

---

## Notes on Data Integrity

- All market sizing anchors are cited from published research. Internal Onyx figures are clearly labeled as third-party estimates.
- Pipeline logs every run with status, row counts, series counts, and git SHA for full auditability.
- Partial failures (one source down) are handled gracefully — available data is still processed and loaded.

---

*Prepared by Steve Goldade · Commercial Analytics · March 2026*
