from pathlib import Path
import sys
import logging
from datetime import datetime, timezone
import json

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
# make src importable
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import bootcamp_data.config as config
from bootcamp_data.io import read_order_csv, read_user_csv, write_parquet
from bootcamp_data.transforms import (
    add_missing_flags,
    normalize_text,
    missingness_report,
    dedupe_keep_latest,
    apply_mapping,
    enforce_order_schema,
    enforce_user_schema,
)
from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
    assert_unique_key,
    assert_in_range,
)

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    paths = config.make_paths(ROOT)
    order_file = paths.raw / "orders.csv"
    user_file = paths.raw / "users.csv"
    logger.info("Loading raw inputs ")
    order_raw = read_order_csv(order_file)
    user_raw = read_user_csv(user_file)
    logger.info("Loaded rows: orders=%s users=%s", len(order_raw), len(user_raw))
    require_columns(
        order_raw,
        [
            "order_id",
            "user_id",
            "amount",
            "quantity",
            "status",
            "created_at",
        ],
    )
    require_columns(
        user_raw,
        [
            "user_id",
            "country",
            "signup_date",
        ],
    )
    assert_non_empty(order_raw)
    assert_non_empty(user_raw)
    orders = enforce_order_schema(order_raw)
    users = enforce_user_schema(user_raw)
    assert_unique_key(users, "user_id", allow_na=False)

    report = missingness_report(orders)
    reports_dir = paths.root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_out = reports_dir / "order_missingness.csv"
    report.to_csv(report_out)
    logger.info("Wrote missingness report to %s", report_out)

    status_norm = normalize_text(orders["status"])
    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    status_clean = apply_mapping(status_norm, mapping)
    orders_clean = orders.assign(status=status_clean).pipe(
        add_missing_flags, cols=["amount", "quantity"]
    )

    assert_in_range(orders_clean["amount"], lo=0, name="amount")
    assert_in_range(orders_clean["quantity"], lo=0, name="quantity")
    write_parquet(orders_clean, paths.processed / "orders_clean.parquet")

    write_parquet(users, paths.processed / "users.parquet")

    logger.info("Wrote cleaned orders and users to %s", paths.processed)


if __name__ == "__main__":
    main()
