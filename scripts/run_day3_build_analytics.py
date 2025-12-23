from pathlib import Path
import sys
import logging
from datetime import datetime, timezone
import json
import pandas as pd

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

logger = logging.getLogger(__name__)


def build_analysis_table(orders: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    return (
        orders.pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
        .pipe(
            safe_left_join,
            users,
            on="user_id",
            validate="many_to_one",
            suffixes=("", "_user"),
        )
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    paths = config.make_paths(ROOT)
    order_file = paths.processed / "orders_clean.parquet"
    user_file = paths.processed / "users.parquet"
    reports_dir = paths.root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    orders = pd.read_parquet(order_file)
    users = pd.read_parquet(user_file)

    require_columns(
        orders,
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
        users,
        [
            "user_id",
            "country",
            "signup_date",
        ],
    )
    assert_non_empty(orders)
    assert_non_empty(users)
    orders = enforce_order_schema(orders)

    users = enforce_user_schema(users)
    assert_unique_key(users, "user_id", allow_na=False)
    order_t = orders.pipe(parse_datetime, col="created_at", utc=True).pipe(
        add_time_parts, ts_col="created_at"
    )

    n_missing_ts = int(order_t["created_at"].isna().sum())
    logger.info(f"Number of orders with missing created_at: {n_missing_ts}")
    joined = safe_left_join(
        order_t,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )
    assert len(joined) == len(order_t), "Join resulted in row count change"
    match_rate = 1.0 - float(joined["country"].isna().mean())
    logger.info(f"User join match rate: {match_rate:.2%}")
    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
    joined = add_outlier_flag(joined, "amount", k=1.5)

    out_path = paths.processed / "analytics_table.parquet"
    write_parquet(joined, out_path)
    logger.info(f"Wrote analysis table to {out_path}")

    # reports (Revenue by country, count of orders by country)
    report = (
        joined.groupby("country", dropna=False)
        .agg(
            total_revenue=pd.NamedAgg(column="amount", aggfunc="sum"),
            order_count=pd.NamedAgg(column="order_id", aggfunc="count"),
        )
        .reset_index()
        .sort_values(by="total_revenue", ascending=False)
    )
    report_path = reports_dir / "revenue_by_country.csv"
    report.to_csv(report_path, index=False)
    logger.info(f"Wrote revenue by country report to {report_path}")


if __name__ == "__main__":
    main()
