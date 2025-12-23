from typing import Literal
import pandas as pd
import re

_ws = re.compile(r"\s+")


def enforce_order_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype(str),
        user_id=df["user_id"].astype(str),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
        status=df["status"].astype("string"),
        created_at=pd.to_datetime(df["created_at"], errors="coerce", utc=True),
    )


def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    """Add common time grouping keys (month, day-of-week, hour, etc.)."""
    ts = df[ts_col]
    return df.assign(
        date=ts.dt.date,
        year=ts.dt.year,
        month=ts.dt.to_period("M").astype("string"),
        dow=ts.dt.day_name(),
        hour=ts.dt.hour,
    )


def iqr_bounds(s: pd.Series, k: float = 1.5) -> tuple[float, float]:
    """Return (lo, hi) IQR bounds for outlier flagging."""
    x = s.dropna()
    q1 = x.quantile(0.25, interpolation="lower")
    q3 = x.quantile(0.75, interpolation="higher")
    iqr = q3 - q1
    return float(q1 - k * iqr), float(q3 + k * iqr)


def count_outliers(s: pd.Series, k: float = 1.5) -> int:
    """Count number of outliers in series based on IQR method."""
    lo, hi = iqr_bounds(s, k)
    return int(((s < lo) | (s > hi)).sum())


def add_outlier_flag(df: pd.DataFrame, col: str, *, k: float = 1.5) -> pd.DataFrame:
    """Add a boolean flag for outliers based on IQR."""
    lo, hi = iqr_bounds(df[col], k=k)
    return df.assign(**{f"{col}__is_outlier": (df[col] < lo) | (df[col] > hi)})


def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    """Cap values to [p_lo, p_hi] (helpful for visualization, not deletion)."""
    x = s.dropna()
    a, b = x.quantile(lo), x.quantile(hi)
    return s.clip(lower=a, upper=b)


def safe_left_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: str | list[str],
    *,
    validate: (
        Literal[
            "one_to_one",
            "1:1",
            "one_to_many",
            "1:m",
            "many_to_one",
            "m:1",
            "many_to_many",
            "m:m",
        ]
        | None
    ),
    suffixes: tuple[str, str] = ("", "_r"),
):
    """Perform a left join with validation and suffixes.

    Args:
        left: Left DataFrame
        right: Right DataFrame
        on: Column(s) to join on
        validate: Validation string for pd.merge (e.g., '')
        suffixes: Suffixes for overlapping columns
    Returns:
        Merged DataFrame
    """
    return pd.merge(
        left,
        right,
        how="left",
        on=on,
        validate=validate,
        suffixes=suffixes,
    )


def enforce_user_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        user_id=df["user_id"].astype(str),
        country=df["country"].astype("string"),
        signup_date=pd.to_datetime(df["signup_date"], errors="coerce", utc=True),
    )


def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    """Create a report of missing values per column.

    Returns:
        DataFrame with n_missing and p_missing columns, sorted by p_missing desc
    """
    n = len(df)
    return (
        df.isna()
        .sum()
        .rename("n_missing")
        .to_frame()
        .assign(p_missing=lambda t: t["n_missing"] / n)
        .sort_values("p_missing", ascending=False)
    )


def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Add boolean columns indicating missing values.

    Args:
        df: Input DataFrame
        cols: Columns to create flags for

    Returns:
        DataFrame with new columns like 'amount__isna' (True/False)
    """
    out = df.copy()
    for c in cols:
        # TODO: Create new column named f"{c}__isna" that is True where c is NA
        out[f"{c}__isna"] = out[c].isna()  # TODO: your code here

    return out


def normalize_text(s: pd.Series) -> pd.Series:
    """Normalize text: strip, casefold, collapse whitespace.

    Args:
        s: Series of text values

    Returns:
        Normalized series
    """
    return (
        s.astype("string")
        # TODO: Strip leading/trailing whitespace
        .str.strip()  # TODO: your code here
        # TODO: Lowercase using casefold
        .str.casefold()  # TODO: your code here
        # TODO: Replace multiple spaces with single space using _ws pattern
        .str.replace(_ws, " ", regex=True)  # TODO: your code here
    )


def apply_mapping(s: pd.Series, mapping: dict) -> pd.Series:
    """Apply value mapping, keeping unmapped values unchanged.

    Args:
        s: Series of values
        mapping: Dict mapping old values to new values

    Returns:
        Series with mapped values
    """
    # TODO: Use .map() with a lambda that looks up in mapping, defaulting to original
    return s.map(lambda x: mapping.get(x, x))  # TODO: your code here


def dedupe_keep_latest(
    df: pd.DataFrame, key_cols: list[str], ts_col: str
) -> pd.DataFrame:
    """Remove duplicates, keeping the latest row by timestamp.

    Args:
        df: DataFrame with potential duplicates
        key_cols: Columns that define uniqueness
        ts_col: Timestamp column to sort by

    Returns:
        Deduplicated DataFrame
    """
    return (
        df
        # TODO: Sort by timestamp column
        .sort_values(ts_col)  # TODO: your code here
        # TODO: Drop duplicates keeping last (latest) row
        .drop_duplicates(subset=key_cols, keep="last")  # TODO: your code here
        # TODO: Reset index
        .reset_index(drop=True)
    )
