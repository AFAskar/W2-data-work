from joblib import Memory
from pathlib import Path

# Set up caching directory (CWD/data/cache/)
CACHE_DIR = Path("./data/cache/")
# Ensure the cache directory exists
CACHE_DIR.mkdir(parents=True, exist_ok=True)
# Initialize joblib memory for caching
memory = Memory(location=str(CACHE_DIR), verbose=0)


def main():
    print("Hello from w2!")


if __name__ == "__main__":
    main()
