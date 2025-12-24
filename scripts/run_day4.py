from tkinter import NO
from typing import Literal
from pathlib import Path
import sys
import logging
from datetime import datetime, timezone
import plotly.express as px
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
from httpx import get
from joblib import Memory

logger = logging.getLogger(__name__)
paths = config.make_paths(ROOT)

memory = Memory(location=paths.cache / "joblib", verbose=0)


@memory.cache
def get_district_map_data(
    district: str,
) -> str:
    district = district.lower()
    # lookup district using OSM API must have addresstype of neighbourhood
    response = get(
        "https://nominatim.openstreetmap.org/search",
        params={
            "q": district,
            "format": "json",
            "addressdetails": 1,
            "limit": 1,
            "extratags": 1,
            "namedetails": 1,
            "accept-language": "ar",
            "featuretype": "neighbourhood",
        },
    )
    return response.json()


def map_district_to_area(
    district_map_data: dict,
) -> Literal["central", "north", "south", "east", "west", "unknown"]:
    if not district_map_data:
        return "unknown"
    lat = float(district_map_data[0]["lat"])
    lon = float(district_map_data[0]["lon"])
    # North is Above 24.77728 lat
    NORTH_BOUNDARY = 24.77728
    # south is Below 24.59848
    SOUTH_BOUNDARY = 24.59848
    # west is Below 46.69277
    WEST_BOUNDARY = 46.69277
    # east is Above 46.77850
    EAST_BOUNDARY = 46.77850
    # Simple logic based on lat/lon to determine area
    if lat > NORTH_BOUNDARY:
        return "north"
    elif lat < SOUTH_BOUNDARY:
        return "south"
    elif lon < WEST_BOUNDARY:
        return "west"
    elif lon > EAST_BOUNDARY:
        return "east"
    else:
        return "central"


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    data = pd.read_csv(paths.raw / "Aqar_data.csv")
    logger.info(f"Read {len(data)} rows from Aqar_data.csv")
    # Normalize text columns (normalize_text takes a series and returns a series)
    data = data.assign(
        Location=data["location"].pipe(normalize_text),
        Type=data["listTitle"].pipe(normalize_text),
    )

    # split location into city and district
    data = data.assign(
        district=data["Location"].str.split("-", n=1).str[0].str.strip(),
        city=data["Location"].str.split("-", n=1).str[1].str.strip(),
    )
    # Remove the word "حي" from district
    data = data.assign(
        district=data["district"].str.replace("حي", "", regex=False).str.strip()
    )
    logger.info("Normalized text columns and extracted city and district")

    # find outliers in Price column
    data = data.pipe(add_outlier_flag, col="price")
    n_outliers = data["price__is_outlier"].sum()
    logger.info(f"Found {n_outliers} outliers in Price column")

    # winsorize Price column for better visualization
    data = data.assign(price_winsorized=data["price"].pipe(winsorize))

    data = data.assign(
        area=data["district"].apply(get_district_map_data).apply(map_district_to_area)
    )
    logger.info("Mapped districts to areas")

    # Average Price for each district
    avg_price_by_district = (
        data.groupby("district")["price_winsorized"]
        .mean()
        .reset_index()
        .rename(columns={"price_winsorized": "avg_price_winsorized"})
    )
    logger.info("Computed average winsorized price by district")

    avg_price_by_area = (
        data.groupby("area")["price_winsorized"]
        .mean()
        .reset_index()
        .rename(columns={"price_winsorized": "avg_price_winsorized"})
    )
    logger.info("Computed average winsorized price by area")

    fig = px.bar(
        avg_price_by_area,
        x="area",
        y="avg_price_winsorized",
        title="Average Winsorized Price by Area",
    )
    fig_path: Path = paths.figures / "price_by_location.html"
    fig_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(fig_path)
    logger.info(f"Wrote figure to {fig_path}")


if __name__ == "__main__":
    main()
    # r = get_district_map_data("الملز")
    # json.dump(
    #     r,
    #     open("district_map.json", "w", encoding="utf-8"),
    #     ensure_ascii=False,
    #     indent=4,
    # )
