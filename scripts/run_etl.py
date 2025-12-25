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
from bootcamp_data.io import (
    read_order_csv,
    read_user_csv,
    write_parquet,
    write_run_metadata,
)
from bootcamp_data.transforms import (
    add_missing_flags,
    normalize_text,
    missingness_report,
    dedupe_keep_latest,
    apply_mapping,
    enforce_order_schema,
    enforce_user_schema,
    parse_datetime,
    add_time_parts,
    winsorize,
    add_outlier_flag,
)
from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
    assert_unique_key,
    assert_in_range,
)
from bootcamp_data.joins import safe_left_join
from bootcamp_data.etl import load_inputs, transfroms, save_outputs

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    start_time = datetime.now(timezone.utc)
    logger.info("ETL job started at %s", start_time.isoformat())
    paths = config.make_paths(ROOT)

    orders, users = load_inputs(
        order_csv=paths.raw / "orders.csv",
        user_csv=paths.raw / "users.csv",
    )
    orders_transformed, users_transformed = transfroms(orders, users)
    save_outputs(
        orders_transformed,
        users_transformed,
        output_path=paths.processed,
    )
    end_time = datetime.now(timezone.utc)
    logger.info("ETL job finished at %s", end_time.isoformat())
    run_metadata = {
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": (end_time - start_time).total_seconds(),
        "rows_processed": {
            "orders": len(orders_transformed),
            "users": len(users_transformed),
        },
    }
    write_run_metadata(run_metadata, paths.cache / "etl_run_metadata.json")


if __name__ == "__main__":
    main()
