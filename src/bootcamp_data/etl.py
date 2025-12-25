from numpy.typing.tests.test_isfile import ROOT
from datetime import datetime, timezone
import logging
from IPython.testing.plugin.ipdoctest import log
import pandas as pd
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

from pathlib import Path

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]


def load_inputs(order_csv: Path, user_csv: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    orders = read_order_csv(order_csv)
    users = read_user_csv(user_csv)

    return orders, users


def transfroms(orders: pd.DataFrame, users: pd.DataFrame):
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
    orders = enforce_order_schema(orders)

    users = enforce_user_schema(users)
    assert_non_empty(orders)
    assert_non_empty(users)
    assert_unique_key(users, "user_id", allow_na=False)
    order_t = orders.pipe(parse_datetime, col="created_at", utc=True).pipe(
        add_time_parts, ts_col="created_at"
    )
    users_t = users.pipe(parse_datetime, col="signup_date", utc=True).pipe(
        add_time_parts, ts_col="signup_date"
    )

    n_missing_ts = int(order_t["created_at"].isna().sum())
    joined = safe_left_join(
        order_t,
        users_t,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )
    assert len(joined) == len(order_t), "Join resulted in row count change"
    match_rate = 1.0 - float(joined["country"].isna().mean())
    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
    joined = add_outlier_flag(joined, "amount", k=1.5)
    return joined, n_missing_ts, match_rate


def save_outputs(
    orders_transformed: pd.DataFrame,
    users_transformed: pd.DataFrame,
    output_path: Path,
) -> None:
    write_parquet(orders_transformed, output_path / "orders.parquet")
    write_parquet(users_transformed, output_path / "users.parquet")
    # missigness report
    missingness_report(orders_transformed).to_csv(
        output_path / "orders_missingness.csv"
    )
    missingness_report(users_transformed).to_csv(output_path / "users_missingness.csv")


def run_etl():

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
    run_etl()
