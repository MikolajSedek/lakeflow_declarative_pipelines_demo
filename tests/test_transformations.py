"""
Unit tests for pure PySpark transformation functions defined in transformations.py.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transformations import (
    add_load_timestamp,
    anonymize_sensitive_data,
    extract_file_name_from_metadata,
    lower_all_column_names,
    remove_nonsense_columns,
)


# ---------------------------------------------------------------------------
# add_load_timestamp
# ---------------------------------------------------------------------------

class TestAddLoadTimestamp:
    def test_adds_default_column(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1, "a")], ["id", "value"])
        result = add_load_timestamp(df)
        assert "load_timestamp" in result.columns

    def test_adds_custom_column_name(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1,)], ["id"])
        result = add_load_timestamp(df, timestamp_col_name="ingested_at")
        assert "ingested_at" in result.columns
        assert "load_timestamp" not in result.columns

    def test_original_columns_preserved(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1, "hello")], ["id", "value"])
        result = add_load_timestamp(df)
        assert "id" in result.columns
        assert "value" in result.columns

    def test_timestamp_column_type(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1,)], ["id"])
        result = add_load_timestamp(df)
        field = next(f for f in result.schema if f.name == "load_timestamp")
        assert isinstance(field.dataType, TimestampType)

    def test_row_count_unchanged(self, spark: SparkSession) -> None:
        data = [(i,) for i in range(5)]
        df = spark.createDataFrame(data, ["id"])
        result = add_load_timestamp(df)
        assert result.count() == 5


# ---------------------------------------------------------------------------
# extract_file_name_from_metadata
# ---------------------------------------------------------------------------

class TestExtractFileNameFromMetadata:
    def _make_df_with_metadata(self, spark: SparkSession):
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("meta", StructType([
                StructField("file_name", StringType()),
            ])),
        ])
        data = [(1, {"file_name": "orders.csv"}), (2, {"file_name": "products.csv"})]
        return spark.createDataFrame(data, schema)

    def test_adds_file_name_column(self, spark: SparkSession) -> None:
        df = self._make_df_with_metadata(spark)
        result = extract_file_name_from_metadata(
            df, metadata_column="meta", file_name_field="file_name"
        )
        assert "file_name" in result.columns

    def test_file_name_values_correct(self, spark: SparkSession) -> None:
        df = self._make_df_with_metadata(spark)
        result = extract_file_name_from_metadata(
            df, metadata_column="meta", file_name_field="file_name"
        )
        file_names = {row["file_name"] for row in result.collect()}
        assert file_names == {"orders.csv", "products.csv"}

    def test_original_columns_preserved(self, spark: SparkSession) -> None:
        df = self._make_df_with_metadata(spark)
        result = extract_file_name_from_metadata(
            df, metadata_column="meta", file_name_field="file_name"
        )
        assert "id" in result.columns

    def test_row_count_unchanged(self, spark: SparkSession) -> None:
        df = self._make_df_with_metadata(spark)
        result = extract_file_name_from_metadata(
            df, metadata_column="meta", file_name_field="file_name"
        )
        assert result.count() == df.count()


# ---------------------------------------------------------------------------
# lower_all_column_names
# ---------------------------------------------------------------------------

class TestLowerAllColumnNames:
    def test_lowercases_all_columns(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1, "x")], ["ID", "Value"])
        result = lower_all_column_names(df)
        assert result.columns == ["id", "value"]

    def test_already_lowercase_unchanged(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1, "x")], ["id", "value"])
        result = lower_all_column_names(df)
        assert result.columns == ["id", "value"]

    def test_mixed_case_columns(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1,)], ["Person_Name"])
        result = lower_all_column_names(df)
        assert "person_name" in result.columns

    def test_row_count_and_values_unchanged(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(42, "hello")], ["ID", "Message"])
        result = lower_all_column_names(df)
        row = result.first()
        assert row["id"] == 42
        assert row["message"] == "hello"


# ---------------------------------------------------------------------------
# remove_nonsense_columns
# ---------------------------------------------------------------------------

class TestRemoveNonsenseColumns:
    def test_removes_specified_column(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1, 99)], ["id", "nonsense_column"])
        result = remove_nonsense_columns(df, columns_to_drop=["nonsense_column"])
        assert "nonsense_column" not in result.columns

    def test_keeps_remaining_columns(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1, 99, "keep")], ["id", "nonsense_column", "important"])
        result = remove_nonsense_columns(df, columns_to_drop=["nonsense_column"])
        assert "id" in result.columns
        assert "important" in result.columns

    def test_missing_column_ignored(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1,)], ["id"])
        result = remove_nonsense_columns(df, columns_to_drop=["nonexistent"])
        assert result.columns == ["id"]

    def test_removes_multiple_columns(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
        result = remove_nonsense_columns(df, columns_to_drop=["a", "c"])
        assert result.columns == ["b"]

    def test_row_count_unchanged(self, spark: SparkSession) -> None:
        data = [(i, i * 10) for i in range(5)]
        df = spark.createDataFrame(data, ["id", "nonsense_column"])
        result = remove_nonsense_columns(df, columns_to_drop=["nonsense_column"])
        assert result.count() == 5


# ---------------------------------------------------------------------------
# anonymize_sensitive_data
# ---------------------------------------------------------------------------

class TestAnonymizeSensitiveData:
    def test_sensitive_columns_are_hashed(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [(1, "user@example.com")],
            ["id", "personal_email"],
        )
        result = anonymize_sensitive_data(df, sensitive_cols=["personal_email"])
        row = result.first()
        assert row["personal_email"] != "user@example.com"

    def test_hashed_value_is_hex_string(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [(1, "user@example.com")],
            ["id", "personal_email"],
        )
        result = anonymize_sensitive_data(df, sensitive_cols=["personal_email"])
        hashed = result.first()["personal_email"]
        assert len(hashed) == 64  # SHA-256 produces 64 hex characters

    def test_non_sensitive_columns_unchanged(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [(1, "user@example.com", "Alice")],
            ["id", "personal_email", "name"],
        )
        result = anonymize_sensitive_data(df, sensitive_cols=["personal_email"])
        row = result.first()
        assert row["id"] == 1
        assert row["name"] == "Alice"

    def test_absent_sensitive_column_ignored(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1, "data")], ["id", "value"])
        result = anonymize_sensitive_data(df, sensitive_cols=["personal_email"])
        assert result.columns == ["id", "value"]
        assert result.first()["value"] == "data"

    def test_no_sensitive_columns_returns_original(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1, "data")], ["id", "value"])
        result = anonymize_sensitive_data(df, sensitive_cols=[])
        assert result.first()["value"] == "data"

    def test_multiple_sensitive_columns_hashed(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [(1, "user@example.com", "Smith")],
            ["id", "personal_email", "person_surname"],
        )
        result = anonymize_sensitive_data(
            df, sensitive_cols=["personal_email", "person_surname"]
        )
        row = result.first()
        assert row["personal_email"] != "user@example.com"
        assert row["person_surname"] != "Smith"

    def test_sha512_hash_length(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1, "secret")], ["id", "personal_email"])
        result = anonymize_sensitive_data(
            df, sensitive_cols=["personal_email"], sha_hash_length=512
        )
        hashed = result.first()["personal_email"]
        assert len(hashed) == 128  # SHA-512 produces 128 hex characters

    def test_same_input_same_hash(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [(1, "user@example.com"), (2, "user@example.com")],
            ["id", "personal_email"],
        )
        result = anonymize_sensitive_data(df, sensitive_cols=["personal_email"])
        rows = result.orderBy("id").collect()
        assert rows[0]["personal_email"] == rows[1]["personal_email"]
