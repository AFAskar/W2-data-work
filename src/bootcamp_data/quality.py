import pandas as pd


def require_columns(df: pd.DataFrame, required_columns: list[str]) -> None:
    missing = [col for col in required_columns if col not in df.columns]
    assert not missing, f"DataFrame is missing required columns: {missing}"
    return


def assert_non_empty(df: pd.DataFrame) -> None:
    assert not df.empty, "DataFrame is empty"
    return


def assert_unique_key(df: pd.DataFrame, key: str, *, allow_na: bool = False) -> None:
    """Assert that a column is unique (no duplicates).

    Args:
        df: DataFrame to check
        key: Column name to check for uniqueness
        allow_na: If False, also check that no NA values exist

    Raises:
        AssertionError: If column has duplicates or NAs (when not allowed)
    """
    if not allow_na:
        assert df[key].notna().all(), f"{key} contains NA"

    dupl = df[key].duplicated(keep=False)
    nacheck = df[key].notna()
    dup = dupl & nacheck
    assert not dup.any(), f"{key} not unique; {dup.sum()} duplicate rows"


def assert_in_range(s: pd.Series, *, lo=None, hi=None, name: str = "value") -> None:
    """Assert that series values are within range (ignoring NA).

    Args:
        s: Series to check
        lo: Minimum allowed value (None = no minimum)
        hi: Maximum allowed value (None = no maximum)
        name: Name to use in error message

    Raises:
        AssertionError: If any value is outside range
    """
    x = s.dropna()

    if lo is not None:
        assert (x >= lo).all(), f"{name} below {lo}"

    if hi is not None:
        assert (x <= hi).all(), f"{name} above {hi}"
