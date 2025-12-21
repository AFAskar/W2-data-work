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
from bootcamp_data.transforms import enforce_schema

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    paths = config.make_paths(ROOT)
    orders = enforce_schema(read_order_csv(paths.raw / "orders.csv"))
    users = read_user_csv(paths.raw / "users.csv")
    logger.info("Loaded rows: orders=%s users=%s", len(orders), len(users))
    logger.info("Orders dtypes:\n%s", orders.dtypes)
    out_orders = paths.processed / "orders.parquet"
    out_users = paths.processed / "users.parquet"
    logger.info("Writing orders to %s", out_orders)
    logger.info("Writing users to %s", out_users)

    write_parquet(orders, out_orders)
    write_parquet(users, out_users)

    meta = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "rows": {"orders": len(orders), "users": len(users)},
        "outputs": {
            "orders": str(out_orders),
            "users": str(out_users),
        },
    }
    meta_out = paths.processed / "_run_meta.json"
    meta_out.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    logger.info("Wrote %s", paths.processed)
    logger.info("run metadata %s", meta_out)


if __name__ == "__main__":
    main()
