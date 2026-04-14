"""
Unit tests for pure PySpark transformation functions defined in transformations.py.
"""

import pytest
from modules.transformations import (
    add_load_timestamp,
    anonymize_sensitive_data,
    extract_file_name_from_metadata,
    lower_all_column_names,
    remove_nonsense_columns,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType

pytestmark = pytest.mark.spark

# ---------------------------------------------------------------------------
# add_load_timestamp
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("col_name", ["load_timestamp", "ingested_at", "ts"])
def test_add_load_timestamp_column_is_created(simple_df, col_name: str) -> None:
    """Should add a timestamp column with the given custom name."""
    result = add_load_timestamp(simple_df, timestamp_col_name=col_name)
    assert col_name in result.columns


def test_add_load_timestamp_default_column_name(simple_df) -> None:
    """Should default the timestamp column name to 'load_timestamp'."""
    result = add_load_timestamp(simple_df)
    assert "load_timestamp" in result.columns


def test_add_load_timestamp_column_type_is_timestamp(simple_df) -> None:
    """Should create the timestamp column with TimestampType data type."""
    result = add_load_timestamp(simple_df)
    field = next(f for f in result.schema if f.name == "load_timestamp")
    assert isinstance(field.dataType, TimestampType)


def test_add_load_timestamp_original_columns_preserved(simple_df) -> None:
    """Should preserve all original columns after adding the timestamp."""
    result = add_load_timestamp(simple_df)
    assert {"id", "value"}.issubset(set(result.columns))


def test_add_load_timestamp_row_count_unchanged(simple_df) -> None:
    """Should not alter the row count of the DataFrame."""
    result = add_load_timestamp(simple_df)
    assert result.count() == simple_df.count()


# ---------------------------------------------------------------------------
# extract_file_name_from_metadata
# ---------------------------------------------------------------------------


def test_extract_file_name_column_added(metadata_df) -> None:
    """Should add a 'file_name' column extracted from the metadata struct."""
    result = extract_file_name_from_metadata(
        metadata_df, metadata_column="meta", file_name_field="file_name"
    )
    assert "file_name" in result.columns


def test_extract_file_name_values_correct(metadata_df) -> None:
    """Should extract the correct file name values from nested metadata."""
    result = extract_file_name_from_metadata(
        metadata_df, metadata_column="meta", file_name_field="file_name"
    )
    assert {row["file_name"] for row in result.collect()} == {"orders.csv", "products.csv"}


def test_extract_file_name_original_columns_preserved(metadata_df) -> None:
    """Should keep existing columns intact after extraction."""
    result = extract_file_name_from_metadata(
        metadata_df, metadata_column="meta", file_name_field="file_name"
    )
    assert "id" in result.columns


def test_extract_file_name_row_count_unchanged(metadata_df) -> None:
    """Should not alter the row count of the DataFrame."""
    result = extract_file_name_from_metadata(
        metadata_df, metadata_column="meta", file_name_field="file_name"
    )
    assert result.count() == metadata_df.count()


# ---------------------------------------------------------------------------
# lower_all_column_names
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("input_cols", "expected_cols"),
    [
        (["ID", "Value"], ["id", "value"]),
        (["id", "value"], ["id", "value"]),
        (["Person_Name", "SURNAME"], ["person_name", "surname"]),
        (["MixedCase"], ["mixedcase"]),
    ],
)
def test_lower_all_column_names_columns_are_lowercased(
    spark: SparkSession, input_cols: list, expected_cols: list
) -> None:
    """Should lowercase all column names regardless of original casing."""
    df = spark.createDataFrame([(1,) * len(input_cols)], input_cols)
    result = lower_all_column_names(df)
    assert result.columns == expected_cols


def test_lower_all_column_names_values_unchanged(mixed_case_df) -> None:
    """Should preserve cell values when renaming columns to lowercase."""
    result = lower_all_column_names(mixed_case_df)
    assert result.first()["id"] == 1


def test_lower_all_column_names_row_count_unchanged(mixed_case_df) -> None:
    """Should not alter the row count of the DataFrame."""
    result = lower_all_column_names(mixed_case_df)
    assert result.count() == mixed_case_df.count()


# ---------------------------------------------------------------------------
# remove_nonsense_columns
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("columns_to_drop", "expected_remaining"),
    [
        (("nonsense_column",), {"id", "important"}),
        (("id", "nonsense_column"), {"important"}),
        (("nonexistent",), {"id", "nonsense_column", "important"}),
        ((), {"id", "nonsense_column", "important"}),
    ],
)
def test_remove_nonsense_columns_correct_columns_remain(
    nonsense_df, columns_to_drop: tuple, expected_remaining: set
) -> None:
    """Should drop only the specified columns and silently ignore absent ones."""
    result = remove_nonsense_columns(nonsense_df, columns_to_drop=columns_to_drop)
    assert set(result.columns) == expected_remaining


def test_remove_nonsense_columns_default_drops_nonsense_column(nonsense_df) -> None:
    """Should use NONSENSE_COLUMNS default when columns_to_drop is None."""
    result = remove_nonsense_columns(nonsense_df)
    assert "nonsense_column" not in result.columns
    assert "id" in result.columns
    assert "important" in result.columns


def test_remove_nonsense_columns_row_count_unchanged(nonsense_df) -> None:
    """Should not alter the row count after dropping columns."""
    result = remove_nonsense_columns(nonsense_df, columns_to_drop=("nonsense_column",))
    assert result.count() == nonsense_df.count()


# ---------------------------------------------------------------------------
# anonymize_sensitive_data
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("col_name", ["personal_email", "person_surname", "personal_address"])
def test_anonymize_sensitive_column_is_hashed(sensitive_df, col_name: str) -> None:
    """Should replace each sensitive column's original value with a SHA-2 hash."""
    original = sensitive_df.first()[col_name]
    result = anonymize_sensitive_data(sensitive_df, sensitive_cols=(col_name,))
    assert result.first()[col_name] != original


@pytest.mark.parametrize(
    ("hash_length", "expected_hex_len"),
    [
        (0, 64),  # sha2(..., 0) is an alias for SHA-256 in Spark
        (224, 56),  # 224 bits / 4 = 56 hex chars
        (256, 64),  # 256 bits / 4 = 64 hex chars
        (384, 96),  # 384 bits / 4 = 96 hex chars
        (512, 128),  # 512 bits / 4 = 128 hex chars
    ],
)
def test_anonymize_hash_output_length(
    sensitive_df, hash_length: int, expected_hex_len: int
) -> None:
    """Should produce a hex string whose length matches the chosen SHA-2 variant."""
    result = anonymize_sensitive_data(
        sensitive_df, sensitive_cols=("personal_email",), sha_hash_length=hash_length
    )
    assert len(result.first()["personal_email"]) == expected_hex_len


def test_anonymize_absent_column_is_ignored(simple_df) -> None:
    """Should leave the schema unchanged when sensitive columns are absent."""
    result = anonymize_sensitive_data(simple_df, sensitive_cols=("personal_email",))
    assert result.columns == simple_df.columns


def test_anonymize_non_sensitive_columns_unchanged(sensitive_df) -> None:
    """Should not modify columns that are not listed as sensitive."""
    result = anonymize_sensitive_data(sensitive_df, sensitive_cols=("personal_email",))
    assert result.first()["id"] == 1


def test_anonymize_same_value_produces_same_hash(spark: SparkSession) -> None:
    """Should produce identical hashes for identical input values (deterministic)."""
    df = spark.createDataFrame(
        [(1, "same@example.com"), (2, "same@example.com")],
        ["id", "personal_email"],
    )
    result = anonymize_sensitive_data(df, sensitive_cols=("personal_email",))
    rows = result.orderBy("id").collect()
    assert rows[0]["personal_email"] == rows[1]["personal_email"]


def test_anonymize_no_sensitive_columns_returns_frame_unchanged(simple_df) -> None:
    """Should return the DataFrame unmodified when the sensitive list is empty."""
    result = anonymize_sensitive_data(simple_df, sensitive_cols=())
    assert result.first()["value"] == "a"


def test_anonymize_default_sensitive_cols_are_hashed(sensitive_df) -> None:
    """Should use SENSITIVE_COLUMNS default when sensitive_cols is None."""
    result = anonymize_sensitive_data(sensitive_df)
    original = sensitive_df.first()
    hashed = result.first()
    # All three default sensitive columns should be hashed
    assert hashed["personal_email"] != original["personal_email"]
    assert hashed["person_surname"] != original["person_surname"]
    assert hashed["personal_address"] != original["personal_address"]
    # Non-sensitive column should be unchanged
    assert hashed["id"] == original["id"]


@pytest.mark.parametrize("invalid_length", [1, 128, 255, 513, -1])
def test_anonymize_invalid_hash_length_raises_value_error(
    sensitive_df, invalid_length: int
) -> None:
    """Should raise ValueError when sha_hash_length is not a valid SHA-2 bit length."""
    with pytest.raises(ValueError, match="sha_hash_length must be one of"):
        anonymize_sensitive_data(
            sensitive_df, sensitive_cols=("personal_email",), sha_hash_length=invalid_length
        )


@pytest.mark.parametrize("valid_length", [0, 224, 256, 384, 512])
def test_anonymize_valid_hash_lengths_accepted(sensitive_df, valid_length: int) -> None:
    """Should accept all valid SHA-2 bit lengths without raising."""
    result = anonymize_sensitive_data(
        sensitive_df, sensitive_cols=("personal_email",), sha_hash_length=valid_length
    )
    assert "personal_email" in result.columns
