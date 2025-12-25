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
    # Use around filter to get neighborhoods within 50km of Riyadh center (24.7136, 46.6753)
    overpass_query = f"""
    [out:json][timeout:180];
    (
      node["place"~"neighbourhood|suburb"](around:50000, 24.7136, 46.6753);
      way["place"~"neighbourhood|suburb"](around:50000, 24.7136, 46.6753);
      relation["place"~"neighbourhood|suburb"](around:50000, 24.7136, 46.6753);
    );
    out center;
    """

    response = get(overpass_url, params={"data": overpass_query}, timeout=180)
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


# fall back to OSM API for Missing neighborhoods
@memory.cache
def osm_fallback(neighborhood: str) -> dict | None:
    overpass_url = "https://overpass.private.coffee/api/interpreter"

    overpass_query = f"""
    [out:json][timeout:180];
    (
      node["place"~"neighbourhood|suburb"]["name"="{neighborhood}"];
      way["place"~"neighbourhood|suburb"]["name"="{neighborhood}"];
      relation["place"~"neighbourhood|suburb"]["name"="{neighborhood}"];
    );
    out center;
    """

    response = get(overpass_url, params={"data": overpass_query}, timeout=180)
    response.raise_for_status()
    data = response.json()

    for element in data.get("elements", []):
        if "tags" in element and "name" in element["tags"]:
            if element["type"] == "node":
                lat, lon = element["lat"], element["lon"]
            elif "center" in element:
                lat, lon = element["center"]["lat"], element["center"]["lon"]
            else:
                lat, lon = None, None

            return {
                "name": element["tags"]["name"],
                "lat": lat,
                "lon": lon,
                "osm_id": element["id"],
                "osm_type": element["type"],
            }
    return None


def fallback_fallback(query: str) -> dict | None:
    """Use Nominatim API as a last resort to get neighborhood coordinates"""
    nominatim_url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": query,
        "format": "json",
        "addressdetails": 1,
        "limit": 1,
    }
    response = get(nominatim_url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    if data:
        lat = float(data[0]["lat"])
        lon = float(data[0]["lon"])
        return {
            "name": query,
            "lat": lat,
            "lon": lon,
        }
    return None


def area_boundry(
    lat: float, lon: float
) -> Literal["central", "north", "south", "east", "west"]:
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
        location=data["location"].pipe(normalize_text),
        listTitle=data["listTitle"].pipe(normalize_text),
    )

    # split location into city and neighborhood
    data = data.assign(
        neighborhood=data["location"].str.split("-", n=1).str[0].str.strip(),
        city=data["location"].str.split("-", n=1).str[1].str.strip(),
    )
    # Remove the word "حي" from neighborhood
    data = data.assign(
        neighborhood=data["neighborhood"].str.replace("حي", "", regex=False).str.strip()
    )
    logger.info("Normalized text columns and extracted city and neighborhood")

    # Get all neighborhoods and map to area
    neighborhoods = get_all_neighborhoods()
    logger.info(f"Fetched {len(neighborhoods)} neighborhoods from Overpass")
    neighborhood_to_area = {}
    for n in neighborhoods:
        if n["lat"] and n["lon"]:
            # Clean name to match data['neighborhood']
            name = n["name"].replace("حي", "").strip()
            neighborhood_to_area[name] = area_boundry(n["lat"], n["lon"])
    # log NA neighborhoods
    na_neighborhoods = set(data["neighborhood"].unique()) - set(
        neighborhood_to_area.keys()
    )
    if na_neighborhoods:
        for neighborhood in na_neighborhoods:
            fallback = osm_fallback(neighborhood)
            if fallback and fallback["lat"] and fallback["lon"]:
                neighborhood_to_area[neighborhood] = area_boundry(
                    fallback["lat"], fallback["lon"]
                )
                logger.info(
                    f"Found fallback area for neighborhood {neighborhood} using a different Overpass Query"
                )
            else:
                logger.warning(
                    f"No fallback found for neighborhood {neighborhood}! using OSM API Fallback"
                )
                nominatim_fallback = fallback_fallback(neighborhood)
                if (
                    nominatim_fallback
                    and nominatim_fallback["lat"]
                    and nominatim_fallback["lon"]
                ):
                    neighborhood_to_area[neighborhood] = area_boundry(
                        nominatim_fallback["lat"], nominatim_fallback["lon"]
                    )
                    logger.info(
                        f"Found fallback area for neighborhood {neighborhood} using Nominatim API"
                    )
                else:
                    logger.error(
                        f"No coordinates found for neighborhood {neighborhood} using Nominatim API!"
                    )

    # recalculate na_neighborhoods
    na_neighborhoods = set(data["neighborhood"].unique()) - set(
        neighborhood_to_area.keys()
    )
    if na_neighborhoods:
        logger.warning(f"Neighborhoods not found in Overpass data: {na_neighborhoods}")
    data["area"] = data["neighborhood"].map(neighborhood_to_area).fillna("unknown")
    logger.info("Added area column")

    # find outliers in Price column
    data = data.pipe(add_outlier_flag, col="price")
    n_outliers = data["price__is_outlier"].sum()
    logger.info(f"Found {n_outliers} outliers in Price column")

    # winsorize Price column for better visualization
    data = data.assign(price_winsorized=data["price"].pipe(winsorize))

    # Average Price for each area
    logger.info("Computed average winsorized price by area")

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
        title="Average Winsorized Price by area",
    )
    fig_path: Path = paths.figures / "price_by_location.html"
    fig_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(fig_path)
    logger.info(f"Wrote figure to {fig_path}")


if __name__ == "__main__":
    main()
