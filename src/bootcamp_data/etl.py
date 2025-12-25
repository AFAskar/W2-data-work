from __future__ import annotations
from joblib import load
from jedi.inference.analysis import add

from datetime import datetime, timezone
import logging
import pandas as pd
import bootcamp_data.config as config
from bootcamp_data.io import (
    read_order_csv,
    read_user_csv,
    write_parquet,
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

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path


logger = logging.getLogger(__name__)


def write_run_metadata(
    cfg: ETLConfig,
    *,
    orders_raw: pd.DataFrame,
    users: pd.DataFrame,
    analytics: pd.DataFrame,
) -> None:
    missing_created_at = (
        int(analytics["created_at"].isna().sum())
        if "created_at" in analytics.columns
        else None
    )
    country_match_rate = (
        1.0 - float(analytics["country"].isna().mean())
        if "country" in analytics.columns
        else None
    )

    meta = {
        "rows in orders raw": int(len(orders_raw)),
        "rows in users": int(len(users)),
        "rows in analytics": int(len(analytics)),
        "missing created_at in analytics": missing_created_at,
        "country match rate in analytics": country_match_rate,
        "etl_run_time": datetime.now(timezone.utc).isoformat(),
        "path_orders_raw": str(cfg.raw_orders),
        "path_users": str(cfg.raw_users),
        "path_out_orders_clean": str(cfg.out_orders_clean),
        "path_out_users": str(cfg.out_users),
        "path_out_analytics": str(cfg.out_analytics),
    }
    return


def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    orders = read_order_csv(cfg.raw_orders)
    users = read_user_csv(cfg.raw_users)

    return orders, users


def transfroms(orders_raw: pd.DataFrame, users: pd.DataFrame):
    require_columns(
        orders_raw,
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
    status_map = {"paid": "paid", "refund": "refund", "refunded": "refund"}

    orders = (
        orders_raw.pipe(enforce_order_schema)
        .assign(
            status_clean=lambda d: apply_mapping(
                normalize_text(d["status"]), status_map
            )
        )
        .pipe(add_missing_flags, cols=["amount", "quantity"])
        .pipe(parse_datetime, col="created_at", utc=True)
    )

    users = users.pipe(enforce_user_schema)
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


def load_outputs(
    *, analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig
) -> None:
    """Write processed artifacts (idempotent)."""
    write_parquet(users, cfg.out_users)
    write_parquet(analytics, cfg.out_analytics)

    user_side_cols = [c for c in users.columns if c != "user_id"]
    cols_to_drop = [c for c in user_side_cols if c in analytics.columns] + [
        c for c in analytics.columns if c.endswith("_user")
    ]
    orders_clean = analytics.drop(columns=cols_to_drop, errors="ignore")
    write_parquet(orders_clean, cfg.out_orders_clean)


def run_etl(cfg: ETLConfig) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    start_time = datetime.now(timezone.utc)
    logger.info("ETL job started at %s", start_time.isoformat())

    orders, users = load_inputs(cfg)
    orders_transformed, users_transformed = transfroms(orders, users)

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
    write_run_metadata(
        cfg,
        orders_raw=orders,
        users=users,
        analytics=orders_transformed,
    )
