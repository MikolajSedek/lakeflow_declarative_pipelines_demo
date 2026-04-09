"""
Unit tests for data generation helper functions defined in 01.create_fake_data.py.

The generate_list_of_rows function is pure (no SparkSession dependency) so
it can be tested without any Spark infrastructure.
"""

import importlib.util
import os
import sys

import pytest
from pyspark.sql import Row

# 01.create_fake_data.py lives in the repo root; add parent dir to sys.path.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Load the module without relying on a live `spark` global (Databricks runtime).
# The Databricks magic-comment cells are never executed outside the notebook
# runner, so plain importlib is sufficient here.
_spec = importlib.util.spec_from_file_location(
    "create_fake_data",
    os.path.join(os.path.dirname(__file__), "..", "01.create_fake_data.py"),
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

generate_list_of_rows = _mod.generate_list_of_rows
FrameConfig = _mod.FrameConfig


# ---------------------------------------------------------------------------
# generate_list_of_rows – common behaviour
# ---------------------------------------------------------------------------

class TestGenerateListOfRowsCommon:
    @pytest.mark.parametrize("row_type", ["users", "products", "orders"])
    def test_returns_correct_number_of_rows(self, row_type: str) -> None:
        rows = generate_list_of_rows(row_type, num_rows=10)
        assert len(rows) == 10

    @pytest.mark.parametrize("row_type", ["users", "products", "orders"])
    def test_returns_list_of_row_objects(self, row_type: str) -> None:
        rows = generate_list_of_rows(row_type, num_rows=3)
        assert all(isinstance(r, Row) for r in rows)

    @pytest.mark.parametrize("row_type", ["users", "products", "orders"])
    def test_ids_are_sequential_from_one(self, row_type: str) -> None:
        n = 5
        rows = generate_list_of_rows(row_type, num_rows=n)
        ids = [r.id for r in rows]
        assert ids == list(range(1, n + 1))

    def test_invalid_type_raises_assertion(self) -> None:
        with pytest.raises(AssertionError):
            generate_list_of_rows("invalid_type", num_rows=1)

    def test_zero_rows_returns_empty_list(self) -> None:
        assert generate_list_of_rows("users", num_rows=0) == []


# ---------------------------------------------------------------------------
# generate_list_of_rows – users
# ---------------------------------------------------------------------------

class TestGenerateUsersRows:
    EXPECTED_FIELDS = {
        "id",
        "Person_Name",
        "person_surname",
        "Personal_Address",
        "city",
        "Country",
        "personal_email",
        "timestamp",
        "nonsense_column",
    }

    def test_users_have_required_fields(self) -> None:
        rows = generate_list_of_rows("users", num_rows=1)
        assert set(rows[0].__fields__) == self.EXPECTED_FIELDS

    def test_nonsense_column_is_int_in_range(self) -> None:
        rows = generate_list_of_rows("users", num_rows=20)
        for row in rows:
            assert 0 <= row.nonsense_column <= 1000

    def test_email_contains_at_sign(self) -> None:
        rows = generate_list_of_rows("users", num_rows=5)
        for row in rows:
            assert "@" in row.personal_email


# ---------------------------------------------------------------------------
# generate_list_of_rows – products
# ---------------------------------------------------------------------------

class TestGenerateProductsRows:
    EXPECTED_FIELDS = {
        "id",
        "product_name",
        "price",
        "description",
        "stock",
        "company_name",
        "timestamp",
        "nonsense_column",
    }

    def test_products_have_required_fields(self) -> None:
        rows = generate_list_of_rows("products", num_rows=1)
        assert set(rows[0].__fields__) == self.EXPECTED_FIELDS

    def test_price_is_within_expected_range(self) -> None:
        rows = generate_list_of_rows("products", num_rows=20)
        for row in rows:
            assert 10.0 <= row.price <= 500.0

    def test_stock_is_non_negative_integer(self) -> None:
        rows = generate_list_of_rows("products", num_rows=20)
        for row in rows:
            assert isinstance(row.stock, int)
            assert row.stock >= 0


# ---------------------------------------------------------------------------
# generate_list_of_rows – orders
# ---------------------------------------------------------------------------

class TestGenerateOrdersRows:
    EXPECTED_FIELDS = {
        "id",
        "productid",
        "price",
        "product_name",
        "timestamp",
        "nonsense_column",
    }

    def test_orders_have_required_fields(self) -> None:
        rows = generate_list_of_rows("orders", num_rows=1)
        assert set(rows[0].__fields__) == self.EXPECTED_FIELDS

    def test_price_is_within_expected_range(self) -> None:
        rows = generate_list_of_rows("orders", num_rows=20)
        for row in rows:
            assert 10.0 <= row.price <= 500.0

    def test_productid_is_positive_integer(self) -> None:
        rows = generate_list_of_rows("orders", num_rows=20)
        for row in rows:
            assert isinstance(row.productid, int)
            assert row.productid > 0


# ---------------------------------------------------------------------------
# FrameConfig
# ---------------------------------------------------------------------------

class TestFrameConfig:
    def test_frame_config_stores_name(self) -> None:
        config = FrameConfig(name="fake_users", df=None)
        assert config.name == "fake_users"

    def test_frame_config_is_immutable(self) -> None:
        config = FrameConfig(name="fake_users", df=None)
        with pytest.raises((AttributeError, TypeError)):
            config.name = "other"  # type: ignore[misc]
