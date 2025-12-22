python scripts/run_day2_clean.py
python -c "import pandas as pd; \
 df=pd.read_parquet('data/processed/orders_clean.parquet'); \
 print(df.columns.tolist()); \
 print(df[\
 ['status','status_clean','amount__isna','quantity__isna'] \
 ].head())"