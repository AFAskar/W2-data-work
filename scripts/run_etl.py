from __future__ import annotations
from pathlib import Path
import sys
import logging
from datetime import datetime, timezone
from plotly import express as px
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
# make src importable
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import bootcamp_data.config as config
from bootcamp_data.etl import run_etl, ETLConfig

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    paths = config.make_paths(ROOT)
    cfg = ETLConfig(
        root=ROOT,
        raw_orders=paths.raw / "orders.csv",
        raw_users=paths.raw / "users.csv",
        out_orders_clean=paths.processed / "orders_clean.parquet",
        out_users=paths.processed / "users.parquet",
        out_analytics=paths.processed / "analytics_table.parquet",
        run_meta=paths.processed / "_run_meta.json",
    )
    run_etl(cfg)


if __name__ == "__main__":
    main()
