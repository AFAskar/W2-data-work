from typing import Literal
import pandas as pd


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
