# Databricks notebook source
# MAGIC %pip install mimesis==19.1.0 pendulum==3.2.0 loguru==0.7.3
# MAGIC

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger
from mimesis.enums import Locale
from pyspark.sql import DataFrame

from data_generation import FrameConfig, generate_list_of_rows

NUM_USERS = 2000
NUM_PRODUCTS = 1500
NUM_ORDERS = 3000
LOCALE = Locale.EN
WRITE_PATH = "/Volumes/test_catalog/test_schema/test_volume/fake_source/"


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
def generate_orders_data(num_orders: int = NUM_ORDERS, locale: Locale = LOCALE) -> DataFrame:
    """
    Generates fake orders data.
    """
    orders_rows = generate_list_of_rows("orders", num_orders, locale)
    orders_df = spark.createDataFrame(orders_rows)
    return orders_df


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

# MAGIC %md DDL STATEMENTS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create catalog, schemas and volume
# MAGIC CREATE CATALOG IF NOT EXISTS test_catalog;
# MAGIC CREATE SCHEMA IF NOT EXISTS test_catalog.test_schema;
# MAGIC CREATE VOLUME IF NOT EXISTS test_catalog.test_schema.test_volume;
# MAGIC CREATE SCHEMA IF NOT EXISTS test_catalog.test_bronze_schema;
# MAGIC CREATE SCHEMA IF NOT EXISTS test_catalog.test_silver_schema;
# MAGIC CREATE SCHEMA IF NOT EXISTS test_catalog.test_gold_schema;

# COMMAND ----------

# MAGIC %md RUN PYSPARK CODE

# COMMAND ----------

if __name__ == "__main__":
    fake_configs = [
        FrameConfig("fake_users", generate_users_frame()),
        FrameConfig("fake_products", generate_products_data()),
        FrameConfig("fake_orders", generate_orders_data()),
    ]
    # Spark write actions are I/O-bound and release the GIL, so submitting
    # all three jobs concurrently via threads is faster than writing sequentially.
    write_all_configs_parallel(WRITE_PATH, fake_configs)

# COMMAND ----------
