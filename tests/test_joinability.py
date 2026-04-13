"""
Spark integration tests that verify generated datasets can be joined.

These tests create actual Spark DataFrames from the data generation module and
perform the same joins that the gold-layer KPI tables use, asserting that
every join produces a non-empty result.
"""

import pytest
from pyspark.sql import SparkSession

from data_generation import generate_list_of_rows

pytestmark = pytest.mark.spark

# Small but sufficient row counts for join verification
_NUM_USERS = 20
_NUM_PRODUCTS = 15
_NUM_ORDERS = 50


@pytest.fixture(scope="module")
def users_df(spark: SparkSession):
    """Should provide a users DataFrame for join tests."""
    rows = generate_list_of_rows("users", num_rows=_NUM_USERS)
    return spark.createDataFrame(rows)


@pytest.fixture(scope="module")
def products_df(spark: SparkSession):
    """Should provide a products DataFrame for join tests."""
    rows = generate_list_of_rows("products", num_rows=_NUM_PRODUCTS)
    return spark.createDataFrame(rows)


@pytest.fixture(scope="module")
def orders_df(spark: SparkSession):
    """Should provide an orders DataFrame with valid foreign keys for join tests."""
    rows = generate_list_of_rows(
        "orders",
        num_rows=_NUM_ORDERS,
        num_users=_NUM_USERS,
        num_products=_NUM_PRODUCTS,
    )
    return spark.createDataFrame(rows)


# ---------------------------------------------------------------------------
# Tests: orders ⟶ users join
# ---------------------------------------------------------------------------


def test_orders_inner_join_users_is_not_empty(orders_df, users_df) -> None:
    """Should produce a non-empty result when inner-joining orders to users."""
    joined = orders_df.join(users_df, orders_df["userid"] == users_df["id"], how="inner")
    assert joined.count() > 0


def test_orders_inner_join_users_preserves_all_orders(orders_df, users_df) -> None:
    """Should match every order to a user (no orphaned foreign keys)."""
    joined = orders_df.join(users_df, orders_df["userid"] == users_df["id"], how="inner")
    assert joined.count() == orders_df.count()


def test_orders_left_join_users_has_no_nulls(orders_df, users_df) -> None:
    """Should produce no null user columns after a left join (all FKs are valid)."""
    joined = orders_df.join(users_df, orders_df["userid"] == users_df["id"], how="left")
    null_count = joined.filter(users_df["id"].isNull()).count()
    assert null_count == 0


# ---------------------------------------------------------------------------
# Tests: orders ⟶ products join
# ---------------------------------------------------------------------------


def test_orders_inner_join_products_is_not_empty(orders_df, products_df) -> None:
    """Should produce a non-empty result when inner-joining orders to products."""
    joined = orders_df.join(
        products_df,
        orders_df["productid"] == products_df["id"],
        how="inner",
    )
    assert joined.count() > 0


def test_orders_inner_join_products_preserves_all_orders(orders_df, products_df) -> None:
    """Should match every order to a product (no orphaned foreign keys)."""
    joined = orders_df.join(
        products_df,
        orders_df["productid"] == products_df["id"],
        how="inner",
    )
    assert joined.count() == orders_df.count()


def test_orders_left_join_products_has_no_nulls(orders_df, products_df) -> None:
    """Should produce no null product columns after a left join (all FKs are valid)."""
    joined = orders_df.join(
        products_df,
        orders_df["productid"] == products_df["id"],
        how="left",
    )
    null_count = joined.filter(products_df["id"].isNull()).count()
    assert null_count == 0


# ---------------------------------------------------------------------------
# Tests: three-way join (orders ⟶ users + orders ⟶ products)
# ---------------------------------------------------------------------------


def test_three_way_join_is_not_empty(orders_df, users_df, products_df) -> None:
    """Should produce a non-empty result from the full three-way join."""
    enriched = orders_df.join(
        users_df,
        orders_df["userid"] == users_df["id"],
        how="inner",
    ).join(
        products_df,
        orders_df["productid"] == products_df["id"],
        how="inner",
    )
    assert enriched.count() > 0


def test_three_way_join_preserves_all_orders(orders_df, users_df, products_df) -> None:
    """Should match every order to both a user and a product."""
    enriched = orders_df.join(
        users_df,
        orders_df["userid"] == users_df["id"],
        how="inner",
    ).join(
        products_df,
        orders_df["productid"] == products_df["id"],
        how="inner",
    )
    assert enriched.count() == orders_df.count()


# ---------------------------------------------------------------------------
# Tests: aggregations on joined data produce results
# ---------------------------------------------------------------------------


def test_revenue_per_product_aggregation_is_not_empty(orders_df, products_df) -> None:
    """Should produce at least one product revenue row after aggregation."""
    import pyspark.sql.functions as F

    joined = orders_df.join(
        products_df,
        orders_df["productid"] == products_df["id"],
        how="inner",
    )
    agg = joined.groupBy(products_df["id"]).agg(
        F.sum(orders_df["price"]).alias("total_revenue"),
        F.count("*").alias("order_count"),
    )
    assert agg.count() > 0


def test_customer_order_summary_aggregation_is_not_empty(orders_df, users_df) -> None:
    """Should produce at least one customer summary row after aggregation."""
    import pyspark.sql.functions as F

    joined = orders_df.join(
        users_df,
        orders_df["userid"] == users_df["id"],
        how="inner",
    )
    agg = joined.groupBy(users_df["id"]).agg(
        F.sum(orders_df["price"]).alias("total_spend"),
        F.count("*").alias("order_count"),
    )
    assert agg.count() > 0
