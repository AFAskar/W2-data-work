import pandas as pd
from pathlib import Path
from httpx import Client
from typing import Optional
import json

import time
import atexit

_default_client: Optional[Client] = None
NA_LIST = ["", "NA", "N/A", "null", "None"]


def _get_default_client() -> Client:
    global _default_client
    if _default_client is None:
        _default_client = Client()
        atexit.register(_default_client.close)
    return _default_client


def read_order_csv(filepath: Path) -> pd.DataFrame:
    df = pd.read_csv(
        filepath,
        na_values=NA_LIST,
        dtype={
            "order_id": "string",
            "user_id": "string",
        },
        keep_default_na=True,
    )
    return df


def read_user_csv(filepath: Path) -> pd.DataFrame:
    df = pd.read_csv(
        filepath,
        na_values=NA_LIST,
        dtype={
            "user_id": str,
        },
        keep_default_na=True,
    )
    return df


def write_parquet(df: pd.DataFrame, outpath: Path):
    outpath.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(outpath, index=False)
    return


def read_parquet(filepath: Path) -> pd.DataFrame:
    df = pd.read_parquet(filepath)
    return df


def get_url(url: str, client: Optional[Client] = None) -> str:
    if client is None:
        client = _get_default_client()

    response = client.get(url)
    response.raise_for_status()
    data = response.json()
    return data


# ttl is optional
def fetch_from_cache(
    url: str,
    cache_path: Path,
    client: Optional[Client] = None,
    ttl: Optional[int] = None,
) -> str:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if client is None:
        client = _get_default_client()

    if cache_path.exists():
        age_s = time.time() - cache_path.stat().st_mtime
        if ttl is None or age_s < ttl:
            data = json.loads(cache_path.read_text())
            return data
    data = get_url(url, client)
    cache_path.write_text(json.dumps(data))

    return data


def outputMD(df: pd.DataFrame, outpath: Path):
    md_content = df.to_markdown()
    outpath.write_text(md_content)
    return


if __name__ == "__main__":
    SRC = Path(__file__).resolve().parents[1]
    import sys

    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))

    from bootcamp_data.config import make_paths

    ROOT_DIR = Path(__file__).resolve().parents[2]
    paths = make_paths(ROOT_DIR)
    cache_dir = paths.cache
    output_dir = paths.processed
    sample_url = "https://jsonplaceholder.typicode.com/posts"
    data = fetch_from_cache(sample_url, cache_dir / "posts.json")
    df = pd.DataFrame(data)
    output_path = output_dir / "output.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    outputMD(df, output_path)
    print(f"Markdown output written to {output_path}")
