"""
pipeline.py
Orchestrates the full ETL run:
  extract  → transform  → load

Run directly:   python pipeline.py
Schedule via:   GitHub Actions (.github/workflows/pipeline.yml) — weekly on Sunday 06:00 UTC
"""

import logging
import time
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from extract   import fetch_all_fred, fetch_all_bls
from transform import run_transforms
from load      import run_load

# ── LOGGING ────────────────────────────────────────────────────────────────────
LOG_PATH = Path(__file__).parent.parent / "data" / "pipeline.log"
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, mode="a"),
    ]
)
log = logging.getLogger(__name__)


def run() -> bool:
    """
    Execute the full pipeline. Returns True on success, False on failure.
    Partial success (some sources failed) returns True with logged warnings.
    """
    start = time.time()
    log.info("=" * 60)
    log.info(f"Onyx Collection Market Pipeline — {datetime.utcnow().isoformat()}Z")
    log.info("=" * 60)

    raw_df  = None
    error   = None

    try:
        # ── EXTRACT ────────────────────────────────────────────────────────────
        log.info("EXTRACT — fetching FRED series...")
        fred_df = fetch_all_fred(start_year=2019)
        log.info(f"  FRED complete: {len(fred_df)} rows across "
                 f"{fred_df['series_id'].nunique()} series")

        log.info("EXTRACT — fetching BLS series...")
        bls_df = fetch_all_bls(start_year=2019)
        log.info(f"  BLS complete:  {len(bls_df)} rows across "
                 f"{bls_df['series_id'].nunique()} series")

        import pandas as pd
        raw_df = pd.concat([fred_df, bls_df], ignore_index=True)
        log.info(f"EXTRACT done — {len(raw_df)} total rows | "
                 f"date range {raw_df['date'].min().date()} → {raw_df['date'].max().date()}")

        if raw_df.empty:
            raise ValueError("No data extracted from any source — aborting")

        # ── TRANSFORM ──────────────────────────────────────────────────────────
        log.info("TRANSFORM — running market context calculations...")
        context, sizing, payload = run_transforms(raw_df)

        # ── LOAD ───────────────────────────────────────────────────────────────
        log.info("LOAD — writing to SQLite and JSON...")
        run_load(raw_df, context, sizing, payload,
                 duration=time.time() - start)

    except Exception as exc:
        error = str(exc)
        log.error(f"Pipeline FAILED: {exc}", exc_info=True)
        if raw_df is not None and not raw_df.empty:
            try:
                import pandas as pd
                context, sizing, payload = run_transforms(raw_df)
                run_load(raw_df, context, sizing, payload,
                         duration=time.time() - start, error=error)
                log.info("Partial results saved despite error")
            except Exception as e2:
                log.error(f"Could not save partial results: {e2}")
        return False

    duration = time.time() - start
    log.info(f"Pipeline SUCCESS — {duration:.1f}s")
    log.info("=" * 60)
    return True


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
