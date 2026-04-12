# Databricks notebook source
# MAGIC %pip install mimesis==19.1.0 pendulum==3.2.0 loguru==0.7.3
# MAGIC

# COMMAND ----------

from pyspark.sql import DataFrame

from data_generation import FrameConfig, generate_list_of_rows

NUM_USERS = 2000
NUM_PRODUCTS = 1500
NUM_ORDERS = 3000
LOCALE = "en"
WRITE_PATH = "/Volumes/test_catalog/test_schema/test_volume/fake_source/"


# Users Data


def generate_users_frame(
    num_users: int = NUM_USERS,
    locale: str = LOCALE,
) -> DataFrame:
    """
    Generates fake users data. Column naming is intentionally inconsistent :).
    """
    users_rows = generate_list_of_rows("users", num_users, locale)
    users_df = spark.createDataFrame(users_rows)
    return users_df


# Products Data
def generate_products_data(num_products: int = NUM_PRODUCTS, locale: str = LOCALE) -> DataFrame:
    """
    Generates fake products data. Column naming is intentionally inconsistent.
    """
    products_rows = generate_list_of_rows("products", num_products, locale)
    products_df = spark.createDataFrame(products_rows)
    return products_df


# Orders Data
def generate_orders_data(num_orders: int = NUM_ORDERS, locale: str = LOCALE) -> DataFrame:
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
    # prepare fake frames
    fake_users_config = FrameConfig("fake_users", generate_users_frame())
    fake_products_config = FrameConfig("fake_products", generate_products_data())
    fake_orders_config = FrameConfig("fake_orders", generate_orders_data())
    # persist frames to volume
    for config in [fake_users_config, fake_products_config, fake_orders_config]:
        write_frame_config_to_path(WRITE_PATH, config)

# COMMAND ----------
