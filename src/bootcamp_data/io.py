import pandas as pd
from pathlib import Path
from httpx import Client
from typing import Optional
import atexit

output_dir = Path("./data/processed/")
input_dir = Path("./data/raw/")
_default_client: Optional[Client] = None


def _get_default_client() -> Client:
    global _default_client
    if _default_client is None:
        _default_client = Client()
        atexit.register(_default_client.close)
    return _default_client


def get_and_parseURL(url: str, client: Optional[Client] = None) -> pd.DataFrame:
    if client is None:
        client = _get_default_client()

    response = client.get(url)
    response.raise_for_status()
    data = response.json()
    df = pd.json_normalize(data)
    return df


def outputMD(df: pd.DataFrame, outpath: Path):
    md_content = df.to_markdown()
    outpath.write_text(md_content)
    return


if __name__ == "__main__":
    sample_url = "https://jsonplaceholder.typicode.com/posts"
    df = get_and_parseURL(sample_url)
    output_path = output_dir / "output.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    outputMD(df, output_path)
    print(f"Markdown output written to {output_path}")
