# Databricks notebook source
"""Databricks notebook that generates fake data and writes it to Volumes."""

# MAGIC %pip install mimesis==19.1.0 pendulum==3.2.0 loguru==0.7.3
# MAGIC

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger
from mimesis.enums import Locale
from modules.data_generation import FrameConfig, generate_list_of_rows
from modules.ddl_helpers import build_setup_ddl_statements
from pyspark.sql import DataFrame, SparkSession

CATALOG = "test_catalog"
SCHEMA = "test_schema"
VOLUME = "test_volume"
BRONZE_SCHEMA = "test_bronze_schema"
SILVER_SCHEMA = "test_silver_schema"
GOLD_SCHEMA = "test_gold_schema"

NUM_USERS = 20000
NUM_PRODUCTS = 15000
NUM_ORDERS = 30000
LOCALE = Locale.EN
WRITE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/fake_source/"


# Users Data


def generate_users_frame(
    num_users: int = NUM_USERS,
    locale: Locale = LOCALE,
) -> DataFrame:
    """
    Generates fake users data. Column naming is intentionally inconsistent :).
    """
    users_rows = generate_list_of_rows("users", num_users, locale)
    users_df = spark.createDataFrame(users_rows)
    return users_df


# Products Data
def generate_products_data(num_products: int = NUM_PRODUCTS, locale: Locale = LOCALE) -> DataFrame:
    """
    Generates fake products data. Column naming is intentionally inconsistent.
    """
    products_rows = generate_list_of_rows("products", num_products, locale)
    products_df = spark.createDataFrame(products_rows)
    return products_df


# Orders Data
def generate_orders_data(
    num_orders: int = NUM_ORDERS,
    locale: Locale = LOCALE,
    num_users: int = NUM_USERS,
    num_products: int = NUM_PRODUCTS,
) -> DataFrame:
    """
    Generates fake orders data with valid foreign keys to users and products.
    """
    orders_rows = generate_list_of_rows(
        "orders",
        num_orders,
        locale,
        num_users=num_users,
        num_products=num_products,
    )
    orders_df = spark.createDataFrame(orders_rows)
    return orders_df


def run_setup_ddl(
    spark: SparkSession,
    catalog: str = CATALOG,
    schema: str = SCHEMA,
    volume: str = VOLUME,
    extra_schemas: tuple[str, ...] = (BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA),
) -> None:
    """Execute catalog/schema/volume provisioning DDL against *spark*.

    Side-effect function — kept at the outermost layer of the call stack so that
    the builder logic (``build_setup_ddl_statements``) remains pure and testable.
    """
    for statement in build_setup_ddl_statements(catalog, schema, volume, extra_schemas):
        logger.info("Executing DDL: {}", statement)
        spark.sql(statement)


def write_frame_config_to_path(root_path: str, config: FrameConfig) -> None:
    """
    Writes a DataFrame to a path.
    """
    config.df.write.mode("append").csv(f"{root_path}/{config.name}", header=True)


def write_all_configs_parallel(
    root_path: str,
    configs: list[FrameConfig],
    max_workers: int = 3,
) -> None:
    """Write multiple FrameConfigs to storage concurrently.

    Each Spark ``.write`` action is I/O-bound and releases the GIL while the
    JVM executes the job, so ``ThreadPoolExecutor`` lets the driver submit all
    write jobs at the same time instead of waiting for each one sequentially.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(write_frame_config_to_path, root_path, cfg): cfg.name for cfg in configs
        }
        for future in as_completed(futures):
            table_name = futures[future]
            future.result()  # re-raises any write exception
            logger.info("Finished writing {}", table_name)


# COMMAND ----------

# MAGIC %md RUN CODE

# COMMAND ----------

# MAGIC %md DDL STATEMENTS — provisioned via run_setup_ddl() below

# COMMAND ----------

# MAGIC %md RUN PYSPARK CODE

# COMMAND ----------

if __name__ == "__main__":
    run_setup_ddl(spark)
    fake_configs = [
        FrameConfig("fake_users", generate_users_frame()),
        FrameConfig("fake_products", generate_products_data()),
        FrameConfig("fake_orders", generate_orders_data()),
    ]
    # Spark write actions are I/O-bound and release the GIL, so submitting
    # all three jobs concurrently via threads is faster than writing sequentially.
    write_all_configs_parallel(WRITE_PATH, fake_configs)

# COMMAND ----------
