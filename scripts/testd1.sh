source ./.venv/bin/activate
uv run scripts/run_day1_load.py
python -c"import pandas as pd; \
           df=pd.read_parquet('data/processed/orders.parquet'); \
           print(df.dtypes); \
           print(df.head())"