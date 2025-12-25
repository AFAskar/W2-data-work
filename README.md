# Week 2 Data Pipeline

This project implements a reproducible ETL pipeline and EDA workflow for analyzing e-commerce order data.

## Project Structure

```
├── data/
│   ├── raw/            # Input CSVs (orders.csv, users.csv)
│   └── processed/      # Output Parquet files + run metadata
├── notebooks/
│   └── eda.ipynb       # Exploratory Data Analysis
├── reports/
│   ├── figures/        # Exported plots
│   └── summary.md      # Executive summary of findings
├── scripts/
│   └── run_etl.py      # Main entrypoint for the pipeline
└── src/
    └── bootcamp_data/  # Python package with ETL logic
```

## How to Run

1.  **Setup Environment**:
    Ensure you have the required dependencies installed (pandas, plotly, pyarrow).

2.  **Run the ETL Pipeline**:
    This script reads raw data, cleans it, joins it with users, and saves processed Parquet files.

    ```bash
    python scripts/run_etl.py
    ```

3.  **Check Outputs**:
    Processed files will appear in `data/processed/`:

    - `analytics_table.parquet`: The main joined table for analysis.
    - `_run_meta.json`: Metadata about the last run (row counts, paths).

4.  **Run Analysis**:
    Open `notebooks/eda.ipynb` to view the analysis and visualizations.

## Outputs

The pipeline generates the following artifacts in `data/processed/`:

- **analytics_table.parquet**: Enriched orders with user details and time features.
- **orders_clean.parquet**: Cleaned orders table.
- **users.parquet**: Cleaned users table.
