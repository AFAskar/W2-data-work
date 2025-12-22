import pandas as pd
import re

_ws = re.compile(r"\s+")


def enforce_order_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype(str),
        user_id=df["user_id"].astype(str),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
        status=df["status"].str.title(),
        created_at=pd.to_datetime(df["created_at"], errors="coerce"),
    )


def enforce_user_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        user_id=df["user_id"].astype(str),
        country=df["country"].astype("string"),
        signup_date=pd.to_datetime(df["signup_date"], errors="coerce"),
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
