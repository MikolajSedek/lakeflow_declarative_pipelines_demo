"""
Unit tests for pure data generation functions defined in data_generation.py.

All tested functions are pure (no SparkSession dependency).
"""

import pytest
from pyspark.sql import Row

from data_generation import FrameConfig, generate_list_of_rows

# ---------------------------------------------------------------------------
# Shared fixture – parametrized over all row types
# ---------------------------------------------------------------------------

@pytest.fixture(params=["users", "products", "orders"])
def any_row_type(request):
    return request.param


# ---------------------------------------------------------------------------
# generate_list_of_rows – common behaviour
# ---------------------------------------------------------------------------

class TestGenerateListOfRowsCommon:
    @pytest.mark.parametrize("num_rows", [0, 1, 5, 10])
    def test_returns_correct_count(self, any_row_type, num_rows: int) -> None:
        rows = generate_list_of_rows(any_row_type, num_rows=num_rows)
        assert len(rows) == num_rows

    def test_returns_list_of_row_objects(self, any_row_type) -> None:
        rows = generate_list_of_rows(any_row_type, num_rows=3)
        assert all(isinstance(r, Row) for r in rows)

    def test_ids_are_sequential_from_one(self, any_row_type) -> None:
        n = 5
        rows = generate_list_of_rows(any_row_type, num_rows=n)
        assert [r.id for r in rows] == list(range(1, n + 1))

    @pytest.mark.parametrize("invalid_type", ["USERS", "invalid", "", "order", " users"])
    def test_invalid_type_raises_value_error(self, invalid_type: str) -> None:
        with pytest.raises(ValueError, match="Invalid row_type"):
            generate_list_of_rows(invalid_type, num_rows=1)


# ---------------------------------------------------------------------------
# generate_list_of_rows – users
# ---------------------------------------------------------------------------

class TestUsersRows:
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

    @pytest.fixture
    def users_rows(self):
        return generate_list_of_rows("users", num_rows=5)

    def test_required_fields(self, users_rows) -> None:
        assert set(users_rows[0].__fields__) == self.EXPECTED_FIELDS

    def test_email_contains_at_sign(self, users_rows) -> None:
        assert all("@" in row.personal_email for row in users_rows)

    def test_nonsense_column_in_range(self, users_rows) -> None:
        assert all(0 <= row.nonsense_column <= 1000 for row in users_rows)


# ---------------------------------------------------------------------------
# generate_list_of_rows – products
# ---------------------------------------------------------------------------

class TestProductsRows:
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

    @pytest.fixture
    def products_rows(self):
        return generate_list_of_rows("products", num_rows=5)

    def test_required_fields(self, products_rows) -> None:
        assert set(products_rows[0].__fields__) == self.EXPECTED_FIELDS

    def test_price_in_expected_range(self, products_rows) -> None:
        assert all(10.0 <= row.price <= 500.0 for row in products_rows)

    def test_stock_is_non_negative_integer(self, products_rows) -> None:
        assert all(isinstance(row.stock, int) and row.stock >= 0 for row in products_rows)


# ---------------------------------------------------------------------------
# generate_list_of_rows – orders
# ---------------------------------------------------------------------------

class TestOrdersRows:
    EXPECTED_FIELDS = {
        "id",
        "productid",
        "price",
        "product_name",
        "timestamp",
        "nonsense_column",
    }

    @pytest.fixture
    def orders_rows(self):
        return generate_list_of_rows("orders", num_rows=5)

    def test_required_fields(self, orders_rows) -> None:
        assert set(orders_rows[0].__fields__) == self.EXPECTED_FIELDS

    def test_price_in_expected_range(self, orders_rows) -> None:
        assert all(10.0 <= row.price <= 500.0 for row in orders_rows)

    def test_productid_is_positive_integer(self, orders_rows) -> None:
        assert all(isinstance(row.productid, int) and row.productid > 0 for row in orders_rows)


# ---------------------------------------------------------------------------
# FrameConfig
# ---------------------------------------------------------------------------

class TestFrameConfig:
    def test_stores_name(self) -> None:
        config = FrameConfig(name="fake_users", df=None)
        assert config.name == "fake_users"

    def test_is_immutable(self) -> None:
        config = FrameConfig(name="fake_users", df=None)
        with pytest.raises((AttributeError, TypeError)):
            config.name = "other"  # type: ignore[misc]
