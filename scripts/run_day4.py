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
def get_all_neighborhoods(city: str = "الرياض") -> list[dict]:
    """use The Overpass API to get all neighborhoods in a city"""
    overpass_url = "http://overpass-api.de/api/interpreter"

    # Overpass QL query to get neighborhoods in the city
    overpass_query = f"""
    [out:json];
    area["name"="{city}"]["admin_level"~"4|5|6"]->.city;
    (
      node["place"~"neighbourhood|suburb"](area.city);
      way["place"~"neighbourhood|suburb"](area.city);
      relation["place"~"neighbourhood|suburb"](area.city);
    );
    out center;
    """

    response = get(overpass_url, params={"data": overpass_query})
    response.raise_for_status()
    data = response.json()

    neighborhoods = []
    for element in data.get("elements", []):
        if "tags" in element and "name" in element["tags"]:
            # Get center coordinates
            if element["type"] == "node":
                lat, lon = element["lat"], element["lon"]
            elif "center" in element:
                lat, lon = element["center"]["lat"], element["center"]["lon"]
            else:
                lat, lon = None, None

            neighborhoods.append(
                {
                    "name": element["tags"]["name"],
                    "lat": lat,
                    "lon": lon,
                    "osm_id": element["id"],
                    "osm_type": element["type"],
                }
            )

    return neighborhoods


def area_boundry(
    lat: float, lon: float
) -> Literal["central", "north", "south", "east", "west", "unknown"]:
    NORTH_BOUNDARY = 24.77728
    SOUTH_BOUNDARY = 24.59848
    WEST_BOUNDARY = 46.69277
    EAST_BOUNDARY = 46.77850
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

    # Average Price for each district
    logger.info("Computed average winsorized price by district")

    avg_price_by_district = (
        data.groupby("district")["price_winsorized"]
        .mean()
        .reset_index()
        .rename(columns={"price_winsorized": "avg_price_winsorized"})
    )
    logger.info("Computed average winsorized price by district")

    fig = px.bar(
        avg_price_by_district,
        x="district",
        y="avg_price_winsorized",
        title="Average Winsorized Price by District",
    )
    fig_path: Path = paths.figures / "price_by_location.html"
    fig_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(fig_path)
    logger.info(f"Wrote figure to {fig_path}")


if __name__ == "__main__":
    main()
