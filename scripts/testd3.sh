python scripts/run_day3_build_analytics.py
python -c"\
import pandas as pd; \
df=pd.read_parquet('data/processed/analytics_table.parquet'); \
cols=['user_id','country','month','amount','amount_winsor']; \
cols.append('amount__is_outlier'); \
print(df.columns.tolist()); \
print(df[cols].head())"