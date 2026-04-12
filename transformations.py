"""
Pure PySpark transformation functions used by the advanced pipeline.

These functions are framework-agnostic (no DLT or Spark-session dependencies)
and can be unit-tested with a local SparkSession.
"""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

METADATA_COLUMN = "_metadata"
FILE_NAME_FIELD = "file_name"

NONSENSE_COLUMNS: tuple[str, ...] = ("nonsense_column",)

SENSITIVE_COLUMNS: tuple[str, ...] = ("personal_email", "personal_address", "person_surname")

_VALID_SHA2_LENGTHS: frozenset[int] = frozenset({0, 224, 256, 384, 512})

__all__ = [
    "add_load_timestamp",
    "anonymize_sensitive_data",
    "extract_file_name_from_metadata",
    "lower_all_column_names",
    "remove_nonsense_columns",
]


def add_load_timestamp(
    input_frame: DataFrame,
    timestamp_col_name: str = "load_timestamp",
) -> DataFrame:
    """Add a column with the current timestamp to *input_frame*.

    Returns:
        DataFrame with the additional timestamp column.
    """
    return input_frame.withColumn(timestamp_col_name, F.current_timestamp())


def extract_file_name_from_metadata(
    input_frame: DataFrame,
    metadata_column: str = METADATA_COLUMN,
    file_name_field: str = FILE_NAME_FIELD,
) -> DataFrame:
    """Extract the file name from the metadata struct column.

    Uses the type-safe ``F.col().getField()`` API instead of ``F.expr()``
    to prevent SQL-injection when parameter values originate from configuration.

    Returns:
        DataFrame with an added column named *file_name_field*.
    """
    return input_frame.withColumn(
        file_name_field,
        F.col(metadata_column).getField(file_name_field),
    )


def lower_all_column_names(input_frame: DataFrame) -> DataFrame:
    """Lowercase all column names in the input frame.

    Returns:
        DataFrame with every column name lowercased.
    """
    lowered_columns = [F.col(column).alias(column.lower()) for column in input_frame.columns]
    return input_frame.select(lowered_columns)


def remove_nonsense_columns(
    input_frame: DataFrame,
    columns_to_drop: tuple[str, ...] | None = None,
) -> DataFrame:
    """Drop the specified columns from the input frame.

    Columns that are absent from the frame are silently ignored.

    Returns:
        DataFrame with the specified columns removed.
    """
    if columns_to_drop is None:
        columns_to_drop = NONSENSE_COLUMNS
    existing_cols = set(input_frame.columns)
    existing = [c for c in columns_to_drop if c in existing_cols]
    return input_frame.drop(*existing)


def anonymize_sensitive_data(
    input_frame: DataFrame,
    sensitive_cols: tuple[str, ...] | None = None,
    sha_hash_length: int = 256,
) -> DataFrame:
    """Replace sensitive column values with their SHA-2 hash.

    Only columns that are present in the frame are transformed.

    Args:
        input_frame: Source DataFrame.
        sensitive_cols: Column names to anonymize (defaults to ``SENSITIVE_COLUMNS``).
        sha_hash_length: SHA-2 bit length.  Must be one of 0, 224, 256, 384, 512.

    Returns:
        DataFrame with sensitive columns replaced by their hash values.

    Raises:
        ValueError: If *sha_hash_length* is not a valid SHA-2 bit length.
    """
    if sha_hash_length not in _VALID_SHA2_LENGTHS:
        raise ValueError(
            f"sha_hash_length must be one of {sorted(_VALID_SHA2_LENGTHS)}, "
            f"got {sha_hash_length}"
        )
    if sensitive_cols is None:
        sensitive_cols = SENSITIVE_COLUMNS
    input_cols = input_frame.columns
    transformation_dict = {
        col: F.sha2(F.col(col), sha_hash_length).alias(col)
        for col in sensitive_cols
        if col in input_cols
    }
    if transformation_dict:
        return input_frame.withColumns(transformation_dict)
    return input_frame
