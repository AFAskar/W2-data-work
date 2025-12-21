import pandas as pd
from pathlib import Path
from joblib import Memory

# Set up caching directory (CWD/data/cache/)
CACHE_DIR = Path("./data/cache/")
# Ensure the cache directory exists
CACHE_DIR.mkdir(parents=True, exist_ok=True)
# Initialize joblib memory for caching
memory = Memory(location=str(CACHE_DIR), verbose=0)


def main():
    print("Hello from w2!")


def outputMD(df: pd.DataFrame, outpath: Path):
    md_content = df.to_markdown()
    outpath.write_text(md_content)
    return


if __name__ == "__main__":
    main()
