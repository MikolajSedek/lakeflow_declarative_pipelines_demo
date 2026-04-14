"""
Unit tests for pure data generation functions defined in data_generation.py.

All tested functions are pure (no SparkSession dependency).
"""

import pytest
from modules.data_generation import FrameConfig, generate_list_of_rows
from pyspark.sql import Row

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def users_rows():
    """Should provide a small batch of generated user rows for testing."""
    return generate_list_of_rows("users", num_rows=5)


@pytest.fixture
def products_rows():
    """Should provide a small batch of generated product rows for testing."""
    return generate_list_of_rows("products", num_rows=5)


@pytest.fixture
def orders_rows():
    """Should provide a small batch of generated order rows for testing."""
    return generate_list_of_rows("orders", num_rows=5, num_users=5, num_products=5)


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
    "userid",
    "productid",
    "price",
    "product_name",
    "timestamp",
    "nonsense_column",
}


@pytest.mark.parametrize("row_type", ["users", "products", "orders"])
@pytest.mark.parametrize("num_rows", [0, 1, 5, 10])
def test_returns_correct_count(row_type: str, num_rows: int) -> None:
    """Should return exactly num_rows rows for every supported row type."""
    kwargs = {}
    if row_type == "orders" and num_rows > 0:
        kwargs = {"num_users": 10, "num_products": 10}
    rows = generate_list_of_rows(row_type, num_rows=num_rows, **kwargs)
    assert len(rows) == num_rows


@pytest.mark.parametrize("row_type", ["users", "products", "orders"])
def test_returns_list_of_row_objects(row_type: str) -> None:
    """Should return a list of PySpark Row objects for every row type."""
    kwargs = {}
    if row_type == "orders":
        kwargs = {"num_users": 10, "num_products": 10}
    rows = generate_list_of_rows(row_type, num_rows=3, **kwargs)
    assert all(isinstance(r, Row) for r in rows)


@pytest.mark.parametrize("row_type", ["users", "products", "orders"])
def test_ids_are_sequential_from_one(row_type: str) -> None:
    """Should assign sequential ids starting from 1."""
    n = 5
    kwargs = {}
    if row_type == "orders":
        kwargs = {"num_users": 10, "num_products": 10}
    rows = generate_list_of_rows(row_type, num_rows=n, **kwargs)
    assert [r.id for r in rows] == list(range(1, n + 1))


@pytest.mark.parametrize("invalid_type", ["USERS", "invalid", "", "order", " users"])
def test_invalid_type_raises_value_error(invalid_type: str) -> None:
    """Should raise ValueError when row_type is not a supported value."""
    with pytest.raises(ValueError, match="Invalid row_type"):
        generate_list_of_rows(invalid_type, num_rows=1)


@pytest.mark.parametrize("negative_num", [-1, -10, -100])
def test_negative_num_rows_raises_value_error(negative_num: int) -> None:
    """Should raise ValueError when num_rows is negative."""
    with pytest.raises(ValueError, match="num_rows must be non-negative"):
        generate_list_of_rows("users", num_rows=negative_num)


# ---------------------------------------------------------------------------
# generate_list_of_rows – users
# ---------------------------------------------------------------------------


def test_users_required_fields(users_rows) -> None:
    """Should generate user rows containing all expected field names."""
    assert set(users_rows[0].__fields__) == EXPECTED_USERS_FIELDS


def test_users_email_contains_at_sign(users_rows) -> None:
    """Should produce valid-looking emails that contain an '@' character."""
    assert all("@" in row.personal_email for row in users_rows)


def test_users_nonsense_column_in_range(users_rows) -> None:
    """Should keep the nonsense_column values within the 0-1000 range."""
    assert all(0 <= row.nonsense_column <= 1000 for row in users_rows)


# ---------------------------------------------------------------------------
# generate_list_of_rows – products
# ---------------------------------------------------------------------------


def test_products_required_fields(products_rows) -> None:
    """Should generate product rows containing all expected field names."""
    assert set(products_rows[0].__fields__) == EXPECTED_PRODUCTS_FIELDS


def test_products_price_in_expected_range(products_rows) -> None:
    """Should produce product prices between 10.0 and 500.0 inclusive."""
    assert all(10.0 <= row.price <= 500.0 for row in products_rows)


def test_products_stock_is_non_negative_integer(products_rows) -> None:
    """Should produce stock values that are non-negative integers."""
    assert all(isinstance(row.stock, int) and row.stock >= 0 for row in products_rows)


# ---------------------------------------------------------------------------
# generate_list_of_rows – orders
# ---------------------------------------------------------------------------


def test_orders_required_fields(orders_rows) -> None:
    """Should generate order rows containing all expected field names."""
    assert set(orders_rows[0].__fields__) == EXPECTED_ORDERS_FIELDS


def test_orders_price_in_expected_range(orders_rows) -> None:
    """Should produce order prices between 10.0 and 500.0 inclusive."""
    assert all(10.0 <= row.price <= 500.0 for row in orders_rows)


def test_orders_productid_is_positive_integer(orders_rows) -> None:
    """Should produce productid values that are positive integers."""
    assert all(isinstance(row.productid, int) and row.productid > 0 for row in orders_rows)


def test_orders_userid_is_positive_integer(orders_rows) -> None:
    """Should produce userid values that are positive integers."""
    assert all(isinstance(row.userid, int) and row.userid > 0 for row in orders_rows)


# ---------------------------------------------------------------------------
# generate_list_of_rows – orders foreign key ranges
# ---------------------------------------------------------------------------


def test_orders_productid_within_num_products_range() -> None:
    """Should constrain productid to [1, num_products] when num_products is given."""
    num_products = 3
    rows = generate_list_of_rows("orders", num_rows=50, num_users=10, num_products=num_products)
    assert all(1 <= row.productid <= num_products for row in rows)


def test_orders_userid_within_num_users_range() -> None:
    """Should constrain userid to [1, num_users] when num_users is given."""
    num_users = 4
    rows = generate_list_of_rows("orders", num_rows=50, num_users=num_users, num_products=10)
    assert all(1 <= row.userid <= num_users for row in rows)


def test_orders_foreign_keys_missing_num_users_raises() -> None:
    """Should raise ValueError when generating orders without num_users."""
    with pytest.raises(ValueError, match="num_users is required"):
        generate_list_of_rows("orders", num_rows=5, num_products=5)


def test_orders_foreign_keys_missing_num_products_raises() -> None:
    """Should raise ValueError when generating orders without num_products."""
    with pytest.raises(ValueError, match="num_products is required"):
        generate_list_of_rows("orders", num_rows=5, num_users=5)


def test_orders_foreign_keys_missing_both_raises() -> None:
    """Should raise ValueError when generating orders without any FK counts."""
    with pytest.raises(ValueError, match="num_users is required"):
        generate_list_of_rows("orders", num_rows=5)


def test_orders_timestamps_vary() -> None:
    """Should produce varied timestamps (spread over 30 days) for SCD2 relevance."""
    rows = generate_list_of_rows("orders", num_rows=20, num_users=10, num_products=10)
    timestamps = {row.timestamp for row in rows}
    # With 20 random timestamps spread over 30 days, we expect some variation
    assert len(timestamps) > 1


def test_orders_zero_num_users_raises_value_error() -> None:
    """Should raise ValueError when generating orders with num_users=0."""
    with pytest.raises(ValueError, match="num_users must be positive"):
        generate_list_of_rows("orders", num_rows=5, num_users=0, num_products=5)


def test_orders_zero_num_products_raises_value_error() -> None:
    """Should raise ValueError when generating orders with num_products=0."""
    with pytest.raises(ValueError, match="num_products must be positive"):
        generate_list_of_rows("orders", num_rows=5, num_users=5, num_products=0)


def test_orders_negative_num_users_raises_value_error() -> None:
    """Should raise ValueError when generating orders with negative num_users."""
    with pytest.raises(ValueError, match="num_users must be positive"):
        generate_list_of_rows("orders", num_rows=5, num_users=-1, num_products=5)


def test_orders_zero_rows_with_zero_foreign_keys_is_allowed() -> None:
    """Should allow zero orders even with zero foreign key counts (no FK to validate)."""
    rows = generate_list_of_rows("orders", num_rows=0, num_users=0, num_products=0)
    assert rows == []


def test_orders_all_userids_are_joinable_to_users() -> None:
    """Should guarantee every order userid exists in the users ID range."""
    num_users = 10
    num_products = 5
    orders = generate_list_of_rows(
        "orders", num_rows=100, num_users=num_users, num_products=num_products
    )
    users = generate_list_of_rows("users", num_rows=num_users)
    user_ids = {u.id for u in users}
    order_user_ids = {o.userid for o in orders}
    # Every userid in orders must exist in the users table
    assert order_user_ids.issubset(user_ids)


def test_orders_all_productids_are_joinable_to_products() -> None:
    """Should guarantee every order productid exists in the products ID range."""
    num_users = 10
    num_products = 5
    orders = generate_list_of_rows(
        "orders", num_rows=100, num_users=num_users, num_products=num_products
    )
    products = generate_list_of_rows("products", num_rows=num_products)
    product_ids = {p.id for p in products}
    order_product_ids = {o.productid for o in orders}
    # Every productid in orders must exist in the products table
    assert order_product_ids.issubset(product_ids)


# ---------------------------------------------------------------------------
# FrameConfig
# ---------------------------------------------------------------------------


def test_frame_config_stores_name() -> None:
    """Should store the given name attribute in FrameConfig."""
    config = FrameConfig(name="fake_users", df=None)
    assert config.name == "fake_users"


def test_frame_config_is_immutable() -> None:
    """Should reject attribute reassignment since FrameConfig is a NamedTuple."""
    config = FrameConfig(name="fake_users", df=None)
    with pytest.raises((AttributeError, TypeError)):
        config.name = "other"  # type: ignore[misc]
