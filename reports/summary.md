# Week 2 Summary — ETL + EDA

## Key findings

- **Data Volume**: Successfully processed **5 orders** and **4 users**.
- **Join Quality**: Achieved a **100% match rate** between orders and users (all orders were successfully linked to a user country).
- **Revenue**: Total revenue was calculated across all valid orders (see `eda.ipynb` for exact figures).

## Definitions

- **Revenue**: Sum of `amount` for all orders.
- **Status Clean**: Normalized status field where "refunded" is mapped to "refund".
- **Country Match Rate**: The percentage of orders that have a non-null `country` after joining with the users table.

## Data quality caveats

- **Small Dataset**: The current analysis is based on a very small sample (5 rows), so statistical significance is limited.
- **Missing Timestamps**: **1 order** was found to have a missing `created_at` timestamp, which may affect time-series analysis.
- **Outliers**: Outlier detection was run on the `amount` column (using IQR method), and a `is_outlier` flag is available in the analytics table.

## Next questions

- Can we get a larger extract of the raw data to validate these patterns?
- Why is the `created_at` timestamp missing for some orders? Is this a system error?
- Are there any orders from users who signed up _after_ the order date? (Requires further temporal validation).
