"""
Unit tests for pure data generation functions defined in data_generation.py.

All tested functions are pure (no SparkSession dependency).
"""

import pytest
from pyspark.sql import Row

from data_generation import FrameConfig, generate_list_of_rows

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def users_rows():
    return generate_list_of_rows("users", num_rows=5)


@pytest.fixture
def products_rows():
    return generate_list_of_rows("products", num_rows=5)


@pytest.fixture
def orders_rows():
    return generate_list_of_rows("orders", num_rows=5)


# ---------------------------------------------------------------------------
# generate_list_of_rows – common behaviour
# ---------------------------------------------------------------------------

EXPECTED_USERS_FIELDS = {
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

EXPECTED_PRODUCTS_FIELDS = {
    "id",
    "product_name",
    "price",
    "description",
    "stock",
    "company_name",
    "timestamp",
    "nonsense_column",
}

EXPECTED_ORDERS_FIELDS = {
    "id",
    "productid",
    "price",
    "product_name",
    "timestamp",
    "nonsense_column",
}


@pytest.mark.parametrize("row_type", ["users", "products", "orders"])
@pytest.mark.parametrize("num_rows", [0, 1, 5, 10])
def test_returns_correct_count(row_type: str, num_rows: int) -> None:
    rows = generate_list_of_rows(row_type, num_rows=num_rows)
    assert len(rows) == num_rows


@pytest.mark.parametrize("row_type", ["users", "products", "orders"])
def test_returns_list_of_row_objects(row_type: str) -> None:
    rows = generate_list_of_rows(row_type, num_rows=3)
    assert all(isinstance(r, Row) for r in rows)


@pytest.mark.parametrize("row_type", ["users", "products", "orders"])
def test_ids_are_sequential_from_one(row_type: str) -> None:
    n = 5
    rows = generate_list_of_rows(row_type, num_rows=n)
    assert [r.id for r in rows] == list(range(1, n + 1))


@pytest.mark.parametrize("invalid_type", ["USERS", "invalid", "", "order", " users"])
def test_invalid_type_raises_value_error(invalid_type: str) -> None:
    with pytest.raises(ValueError, match="Invalid row_type"):
        generate_list_of_rows(invalid_type, num_rows=1)


# ---------------------------------------------------------------------------
# generate_list_of_rows – users
# ---------------------------------------------------------------------------


def test_users_required_fields(users_rows) -> None:
    assert set(users_rows[0].__fields__) == EXPECTED_USERS_FIELDS


def test_users_email_contains_at_sign(users_rows) -> None:
    assert all("@" in row.personal_email for row in users_rows)


def test_users_nonsense_column_in_range(users_rows) -> None:
    assert all(0 <= row.nonsense_column <= 1000 for row in users_rows)


# ---------------------------------------------------------------------------
# generate_list_of_rows – products
# ---------------------------------------------------------------------------


def test_products_required_fields(products_rows) -> None:
    assert set(products_rows[0].__fields__) == EXPECTED_PRODUCTS_FIELDS


def test_products_price_in_expected_range(products_rows) -> None:
    assert all(10.0 <= row.price <= 500.0 for row in products_rows)


def test_products_stock_is_non_negative_integer(products_rows) -> None:
    assert all(isinstance(row.stock, int) and row.stock >= 0 for row in products_rows)


# ---------------------------------------------------------------------------
# generate_list_of_rows – orders
# ---------------------------------------------------------------------------


def test_orders_required_fields(orders_rows) -> None:
    assert set(orders_rows[0].__fields__) == EXPECTED_ORDERS_FIELDS


def test_orders_price_in_expected_range(orders_rows) -> None:
    assert all(10.0 <= row.price <= 500.0 for row in orders_rows)


def test_orders_productid_is_positive_integer(orders_rows) -> None:
    assert all(isinstance(row.productid, int) and row.productid > 0 for row in orders_rows)


# ---------------------------------------------------------------------------
# FrameConfig
# ---------------------------------------------------------------------------


def test_frame_config_stores_name() -> None:
    config = FrameConfig(name="fake_users", df=None)
    assert config.name == "fake_users"


def test_frame_config_is_immutable() -> None:
    config = FrameConfig(name="fake_users", df=None)
    with pytest.raises((AttributeError, TypeError)):
        config.name = "other"  # type: ignore[misc]
