from datetime import datetime
from decimal import Decimal

import pyspark.sql.functions as F
from pyspark.sql.column import Column
from typing_extensions import Optional, Union


def normalize_string(
    col: Column, default: Optional[str] = "unknown", *cols: Column
) -> Column:
    """
    Normalize string columns by coalescing multiple columns, filling null values,
    trimming whitespace, and converting to lowercase.

    This function applies a left-to-right fallback strategy:
    it returns the first non-null value among the provided columns.
    If all columns are null, the default value is used (if provided).

    Parameters
    ----------
    col : Column
        Primary column to normalize.
    default : str, optional
        Value used when all provided columns are null.
        If None, no default value is applied (result may remain null).
        Default is "unknown".
    *cols : Column
        Additional columns used as fallback values, evaluated in order.

    Returns
    -------
    Column
        A Column expression with:
        - null values replaced using fallback logic
        - leading/trailing whitespace removed
        - all characters converted to lowercase

    Notes
    -----
    - Fallback priority is evaluated from left to right:
      col -> *cols -> default
    - Transformation order:
      coalesce -> trim -> lower

    Examples
    --------
    Basic usage:
    >>> df.select(normalize_string(F.col("name"))).show()
    +----------------+
    |normalize_string|
    +----------------+
    |           alice|
    |         unknown|
    +----------------+

    Using multiple fallback columns:
    >>> df.select(normalize_string(F.col("a"), "n/a", F.col("b"), F.col("c")))
    # Priority: a -> b -> c -> "n/a"

    Without default:
    >>> df.select(normalize_string(F.col("a"), None, F.col("b")))
    # Returns null if both a and b are null
    """
    fallback_cols = [col] + list(cols)

    if default is not None:
        fallback_cols.append(F.lit(default))

    return F.trim(F.lower(F.coalesce(*fallback_cols)))


def normalize_numeric(
    col: Column, default: Optional[Union[int, float, Decimal]] = 0, *cols: Column
) -> Column:
    """
    Normalize numeric columns by replacing null values using fallback columns
    and/or a default value.

    This function applies a left-to-right fallback strategy:
    it returns the first non-null value among the provided columns.
    If all columns are null, the default value is used (if provided).

    Parameters
    ----------
    col : Column
        Primary column to normalize.
    default : int, float, or Decimal, optional
        Value used when all provided columns are null.
        If None, no default value is applied (result may remain null).
        Default is 0.
    *cols : Column
        Additional columns used as fallback values, evaluated in order.

    Returns
    -------
    Column
        A Column expression where null values are replaced by the first non-null
        value among the input columns or the default value.

    Notes
    -----
    - Fallback priority is evaluated from left to right:
      col -> *cols -> default
    - This function does not perform type casting; all columns should be of
      compatible numeric types.

    Examples
    --------
    Basic usage with default:
    >>> df.select(normalize_numeric(F.col("distance"))).show()
    +-----------------+
    |normalize_numeric|
    +-----------------+
    |               10|
    |                0|
    +-----------------+

    Using multiple fallback columns:
    >>> df.select(normalize_numeric(F.col("a"), 0, F.col("b"), F.col("c")))
    # Priority: a -> b -> c -> 0

    Without default (only fallback columns):
    >>> df.select(normalize_numeric(F.col("a"), None, F.col("b")))
    # Returns null if both a and b are null
    """
    fallback_cols = [col] + list(cols)

    if default is not None:
        fallback_cols.append(F.lit(default))

    return F.coalesce(*fallback_cols)


def normalize_timestamp(
    col: Column, default: Optional[Union[datetime, str, Column]] = None, *cols: Column
) -> Column:
    """
    Normalize timestamp columns by replacing null values using fallback columns
    and/or a default value.

    This function applies a left-to-right fallback strategy:
    it returns the first non-null value among the provided columns.
    If all columns are null, the default value is used (if provided).

    Parameters
    ----------
    col : Column
        Primary timestamp column to normalize.
    default : datetime, str, or Column, optional
        Value used when all provided columns are null.
        - datetime: converted to timestamp literal
        - str: parsed using Spark's default timestamp format
        - Column: used directly (e.g., F.current_timestamp())
        If None, no default is applied.
    *cols : Column
        Additional timestamp columns used as fallback values, evaluated in order.

    Returns
    -------
    Column
        A Column expression where null values are replaced by the first non-null
        value among the input columns or the default value.

    Notes
    -----
    - Fallback priority: col -> *cols -> default
    - If `default` is a string, it should be in a format recognized by Spark
      (e.g., "yyyy-MM-dd HH:mm:ss").
    - No implicit casting is enforced; ensure compatible types.

    Examples
    --------
    Using string default:
    >>> df.select(normalize_timestamp(F.col("ts"), "1970-01-01 00:00:00"))

    Using current timestamp:
    >>> df.select(normalize_timestamp(F.col("ts"), F.current_timestamp()))

    Using multiple fallback columns:
    >>> df.select(normalize_timestamp(F.col("a"), "1970-01-01 00:00:00", F.col("b"), F.col("c")))
    # Priority: a -> b -> c -> default

    Without default:
    >>> df.select(normalize_timestamp(F.col("a"), None, F.col("b")))
    # Returns null if all columns are null
    """

    fallback_cols = [col] + list(cols)

    if default is not None:
        if isinstance(default, Column):
            fallback_cols.append(default)
        elif isinstance(default, str):
            fallback_cols.append(F.to_timestamp(F.lit(default)))
        else:
            fallback_cols.append(F.lit(default))

    return F.coalesce(*fallback_cols)


def normalize_date(
    col: Column,
    *cols: Column,
    default: Optional[Union[datetime, str, Column]] = None,
) -> Column:
    """
    Normalize date columns by replacing null values using fallback columns
    and/or a default value.

    Fallback priority: col -> *cols -> default
    """

    fallback_cols = [col] + list(cols)

    if default is not None:
        if isinstance(default, Column):
            fallback_cols.append(default)
        elif isinstance(default, str):
            fallback_cols.append(F.to_date(F.lit(default)))
        elif isinstance(default, datetime):
            fallback_cols.append(F.to_date(F.lit(default)))
        else:
            fallback_cols.append(F.lit(default).cast("date"))

    return F.coalesce(*fallback_cols)


if __name__ == "__main__":
    pass
