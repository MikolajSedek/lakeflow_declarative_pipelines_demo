"""
Pure PySpark transformation functions used by the advanced pipeline.

These functions are framework-agnostic (no DLT or Spark-session dependencies)
and can be unit-tested with a local SparkSession.
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

METADATA_COLUMN = "_metadata"
FILE_NAME_FIELD = "file_name"

NONSENSE_COLUMNS = ["nonsense_column"]

SENSITIVE_COLUMNS = ["personal_email", "personal_address", "person_surname"]


def add_load_timestamp(
    input_frame: DataFrame,
    timestamp_col_name: str = "load_timestamp",
) -> DataFrame:
    """
    Adds a new column with the current timestamp.
    """
    return input_frame.withColumn(timestamp_col_name, F.current_timestamp())


def extract_file_name_from_metadata(
    input_frame: DataFrame,
    metadata_column: str = METADATA_COLUMN,
    file_name_field: str = FILE_NAME_FIELD,
) -> DataFrame:
    """
    Extracts the file name from the metadata column and adds it as a new column.
    """
    return input_frame.withColumn("file_name", F.expr(f"{metadata_column}.{file_name_field}"))


def lower_all_column_names(input_frame: DataFrame) -> DataFrame:
    """
    Lowercases all column names in the input frame.
    """
    lowered_columns = [F.col(column).alias(column.lower()) for column in input_frame.columns]
    return input_frame.select(lowered_columns)


def remove_nonsense_columns(
    input_frame: DataFrame,
    columns_to_drop: list[str] = NONSENSE_COLUMNS,
) -> DataFrame:
    """
    Drops the specified columns from the input frame.
    Columns that are absent from the frame are silently ignored.
    """
    existing_cols = set(input_frame.columns)
    existing = [c for c in columns_to_drop if c in existing_cols]
    return input_frame.drop(*existing)


def anonymize_sensitive_data(
    input_frame: DataFrame,
    sensitive_cols: list[str] = SENSITIVE_COLUMNS,
    sha_hash_length: int = 256,
) -> DataFrame:
    """
    Replaces sensitive column values with their SHA-2 hash.
    Only columns that are present in the frame are transformed.
    """
    input_cols = input_frame.columns
    transformation_dict = {
        col: F.sha2(F.col(col), sha_hash_length).alias(col)
        for col in sensitive_cols
        if col in input_cols
    }
    if transformation_dict:
        return input_frame.withColumns(transformation_dict)
    return input_frame
