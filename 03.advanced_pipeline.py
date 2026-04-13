"""Advanced Lakeflow Declarative Pipeline with bronze/silver/gold medallion architecture."""

from functools import reduce

import pyspark.sql.functions as F
from pydantic.dataclasses import dataclass
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from transformations import (
    add_load_timestamp,
    anonymize_sensitive_data,
    extract_file_name_from_metadata,
    lower_all_column_names,
    remove_nonsense_columns,
)

# configuration

SOURCE_FORMAT = "csv"
SOURCE_PATH_ROOT = "/Volumes/test_catalog/test_schema/test_volume/fake_source/"

TARGET_CATALOG = "test_catalog"
BRONZE_SCHEMA = "test_bronze_schema"
SILVER_SCHEMA = "test_silver_schema"
GOLD_SCHEMA = "test_gold_schema"

TABLES_NAMES: tuple[str, ...] = ("fake_orders", "fake_products", "fake_users")

PRIME_KEY_COLUMNS: tuple[str, ...] = ("id",)
TIMESTAMP_COLUMN = "timestamp"

# table name suffixes - extracted as constants to avoid inline magic strings
# (Palantir PySpark style guide: avoid literal strings in logic)
BRONZE_TABLE_SUFFIX = "_raw"
SILVER_TABLE_SUFFIX = "_staging"
GOLD_TABLE_SUFFIX = "_clean"


# configuration object


@dataclass(frozen=True)
class TablePipelineConfig:
    """Configuration for a single table pipeline."""

    table_name: str
    root_source_path: str = SOURCE_PATH_ROOT
    target_catalog: str = TARGET_CATALOG
    bronze_schema: str = BRONZE_SCHEMA
    silver_schema: str = SILVER_SCHEMA
    gold_schema: str = GOLD_SCHEMA
    prime_key_columns: tuple[str, ...] = PRIME_KEY_COLUMNS
    timestamp_column: str = TIMESTAMP_COLUMN


# declarative stream tables, in a production code extract them to a module


def create_raw_bronze_table(
    bronze_table_name: str,
    table_path: str,
    source_format: str = SOURCE_FORMAT,
    header: bool = True,
    infer_schema: bool = True,
) -> None:
    """
    Creates a bronze streaming table from a given source with basic transforms.
    """

    @dp.table(
        name=bronze_table_name,
        comment=(
            f"Bronze streaming table: raw {source_format.upper()} file ingestion"
            " with load timestamp and source file metadata"
        ),
    )
    def raw_bronze_table():
        raw_source = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", source_format)
            .option("header", header)
            .option("inferSchema", infer_schema)
            .load(table_path)
        )
        transformed_source = raw_source.transform(extract_file_name_from_metadata).transform(
            add_load_timestamp
        )
        return transformed_source


def create_silver_staging_table(silver_table_name: str, bronze_table_path: str) -> None:
    """
    Creates a silver streaming staging table from a bronze table.
    """

    @dp.table(
        name=silver_table_name,
        comment=(
            "Silver streaming table: cleaned, column-lowercased,"
            " and anonymized data from the bronze layer"
        ),
    )
    def silver_staging_table():
        bronze_streaming_frame = spark.readStream.table(bronze_table_path)
        silver_table = (
            bronze_streaming_frame.transform(remove_nonsense_columns)
            .transform(lower_all_column_names)
            .transform(anonymize_sensitive_data)
        )
        return silver_table


def create_gold_merged_table(
    silver_table_name: str,
    gold_table_name: str,
    prime_key_columns: tuple[str, ...] = PRIME_KEY_COLUMNS,
    timestamp_column: str = TIMESTAMP_COLUMN,
) -> None:
    """
    Creates a gold table from silver staging table using CDC merge.

    Note: dp.create_auto_cdc_flow is a Databricks-only API and is not available
    in open-source Apache Spark's pyspark.pipelines module.
    """
    dp.create_streaming_table(
        name=gold_table_name,
        comment=(
            f"Gold streaming table: CDC-merged records keyed by "
            f"{', '.join(prime_key_columns)}, sequenced by {timestamp_column}"
        ),
    )
    dp.create_auto_cdc_flow(
        source=silver_table_name,
        target=gold_table_name,
        keys=list(prime_key_columns),
        sequence_by=timestamp_column,
    )


def run_single_pipeline(
    table_config: TablePipelineConfig,
) -> None:
    """
    Create a single table pipeline (bronze → silver → gold).
    """
    source_table_path = f"{table_config.root_source_path}/{table_config.table_name}"
    bronze_prefix = f"{table_config.target_catalog}.{table_config.bronze_schema}"
    silver_prefix = f"{table_config.target_catalog}.{table_config.silver_schema}"
    gold_prefix = f"{table_config.target_catalog}.{table_config.gold_schema}"

    bronze_table_name = f"{bronze_prefix}.{table_config.table_name}{BRONZE_TABLE_SUFFIX}"
    silver_table_name = f"{silver_prefix}.{table_config.table_name}{SILVER_TABLE_SUFFIX}"
    gold_table_name = f"{gold_prefix}.{table_config.table_name}{GOLD_TABLE_SUFFIX}"

    create_raw_bronze_table(bronze_table_name, source_table_path)
    create_silver_staging_table(silver_table_name, bronze_table_name)
    create_gold_merged_table(
        silver_table_name=silver_table_name,
        gold_table_name=gold_table_name,
        prime_key_columns=table_config.prime_key_columns,
        timestamp_column=table_config.timestamp_column,
    )


def run_all_pipelines(table_names: tuple[str, ...] = TABLES_NAMES) -> None:
    """
    Runs all pipelines for a given list of tables.
    """
    for table_name in table_names:
        table_config = TablePipelineConfig(table_name=table_name)
        run_single_pipeline(table_config)


def aggregate_gold_tables(
    tables_names: tuple[str, ...] = TABLES_NAMES,
    target_catalog: str = TARGET_CATALOG,
    gold_schema: str = GOLD_SCHEMA,
    aggregate_table_name: str = "summary_statistics_gold",
    gold_table_postfix: str = GOLD_TABLE_SUFFIX,
    id_column: str = "id",
) -> None:
    """
    Aggregates gold tables stats into a single materialized view table.
    """
    if not tables_names:
        return

    tables_paths = [
        f"{target_catalog}.{gold_schema}.{table_name}{gold_table_postfix}"
        for table_name in tables_names
    ]
    gold_table_name = f"{target_catalog}.{gold_schema}.{aggregate_table_name}"

    @dp.materialized_view(
        name=gold_table_name,
        comment=(
            "Gold summary statistics: row counts and unique ID counts"
            " aggregated across all gold tables"
        ),
    )
    def summary_statistics_table():
        frames_list = [
            spark.read.table(table_path)
            .agg(
                F.count("*").alias("table_rows"),
                F.countDistinct(id_column).alias("unique_ids"),
            )
            .withColumn("table_name", F.lit(table_path))
            for table_path in tables_paths
        ]
        reduced_frame = reduce(DataFrame.union, frames_list)
        return reduced_frame


# ---------------------------------------------------------------------------
# Gold KPI tables - joined & aggregated business views
# ---------------------------------------------------------------------------

# Fully-qualified gold table paths used by KPI views
_ORDERS_GOLD = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.fake_orders{GOLD_TABLE_SUFFIX}"
_PRODUCTS_GOLD = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.fake_products{GOLD_TABLE_SUFFIX}"
_USERS_GOLD = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.fake_users{GOLD_TABLE_SUFFIX}"


def create_gold_revenue_per_product(
    target_catalog: str = TARGET_CATALOG,
    gold_schema: str = GOLD_SCHEMA,
) -> None:
    """Create a materialized view with revenue KPIs aggregated by product.

    Joins orders with products on ``productid == id`` and computes:
    * **total_revenue** - sum of order prices per product
    * **order_count** - number of orders per product
    * **avg_order_value** - average order price per product
    """
    table_name = f"{target_catalog}.{gold_schema}.revenue_per_product_gold"

    @dp.materialized_view(
        name=table_name,
        comment=(
            "Gold KPI: revenue, order count, and average order value"
            " aggregated per product (orders ⟶ products join)"
        ),
    )
    def revenue_per_product():
        orders = spark.read.table(_ORDERS_GOLD)
        products = spark.read.table(_PRODUCTS_GOLD)

        joined = orders.join(products, orders["productid"] == products["id"], how="inner")

        result = joined.groupBy(
            products["id"].alias("product_id"),
            products["product_name"],
            products["company_name"],
        ).agg(
            F.sum(orders["price"]).alias("total_revenue"),
            F.count("*").alias("order_count"),
            F.round(F.avg(orders["price"]), 2).alias("avg_order_value"),
        )
        return result


def create_gold_customer_order_summary(
    target_catalog: str = TARGET_CATALOG,
    gold_schema: str = GOLD_SCHEMA,
) -> None:
    """Create a materialized view with order KPIs aggregated by customer.

    Joins orders with users on ``userid == id`` and computes:
    * **total_spend** - total amount spent per customer
    * **order_count** - number of orders per customer
    * **avg_order_value** - average order price per customer
    """
    table_name = f"{target_catalog}.{gold_schema}.customer_order_summary_gold"

    @dp.materialized_view(
        name=table_name,
        comment=(
            "Gold KPI: total spend, order count, and average order value"
            " aggregated per customer (orders ⟶ users join)"
        ),
    )
    def customer_order_summary():
        orders = spark.read.table(_ORDERS_GOLD)
        users = spark.read.table(_USERS_GOLD)

        joined = orders.join(users, orders["userid"] == users["id"], how="inner")

        result = joined.groupBy(
            users["id"].alias("customer_id"),
            users["person_name"],
            users["city"],
            users["country"],
        ).agg(
            F.sum(orders["price"]).alias("total_spend"),
            F.count("*").alias("order_count"),
            F.round(F.avg(orders["price"]), 2).alias("avg_order_value"),
        )
        return result


def create_gold_orders_enriched(
    target_catalog: str = TARGET_CATALOG,
    gold_schema: str = GOLD_SCHEMA,
) -> None:
    """Create a materialized view that enriches orders with user and product details.

    Performs a three-way join (orders ⟶ users, orders ⟶ products) to produce a
    wide fact table suitable for BI dashboards and ad-hoc analysis.
    """
    table_name = f"{target_catalog}.{gold_schema}.orders_enriched_gold"

    @dp.materialized_view(
        name=table_name,
        comment=(
            "Gold enriched fact table: orders joined with user and product"
            " details for BI dashboards and ad-hoc analysis"
        ),
    )
    def orders_enriched():
        orders = spark.read.table(_ORDERS_GOLD)
        users = spark.read.table(_USERS_GOLD)
        products = spark.read.table(_PRODUCTS_GOLD)

        enriched = (
            orders.join(users, orders["userid"] == users["id"], how="left")
            .join(products, orders["productid"] == products["id"], how="left")
            .select(
                orders["id"].alias("order_id"),
                orders["userid"],
                users["person_name"].alias("customer_name"),
                users["city"].alias("customer_city"),
                users["country"].alias("customer_country"),
                orders["productid"],
                products["product_name"],
                products["company_name"].alias("product_company"),
                orders["price"].alias("order_price"),
                products["price"].alias("product_list_price"),
                orders["timestamp"].alias("order_timestamp"),
            )
        )
        return enriched


def create_all_gold_kpi_tables(
    target_catalog: str = TARGET_CATALOG,
    gold_schema: str = GOLD_SCHEMA,
) -> None:
    """Register all gold-layer KPI materialized views."""
    create_gold_revenue_per_product(target_catalog, gold_schema)
    create_gold_customer_order_summary(target_catalog, gold_schema)
    create_gold_orders_enriched(target_catalog, gold_schema)


if __name__ == "__main__":
    run_all_pipelines()
    aggregate_gold_tables()
    create_all_gold_kpi_tables()
