# Databricks notebook source
# MAGIC %pip install mimesis pendulum logzero
# MAGIC

# COMMAND ----------

import random
from typing import NamedTuple

import pendulum
from logzero import logger 
from mimesis import Person, Address, Generic, Finance
from pyspark.sql import Row, DataFrame


NUM_USERS = 2000
NUM_PRODUCTS = 1500
NUM_ORDERS = 3000
LOCALE = "en"
WRITE_PATH = f"/Volumes/test_catalog/test_schema/test_volume/fake_source/"


def generate_list_of_rows(
    type: str, # could be users, orders, products
    num_rows: int,
    locale: str = LOCALE,
) -> list[Row]:
    """
    Generatest a list of rows for a given type
    """
    assert type in ["users", "products", "orders"], f"Invalid type: {type}"

    person = Person(locale)
    address = Address(locale)
    generic = Generic(locale)
    finance = Finance(locale)
    
    # return users rows
    if type == "users":
        logger.info(f"Generating {num_rows} users rows")
        return [
        # inconsistent naming of columns is intentional :)
        Row(
            id=i,
            Person_Name=person.name(),
            person_surname=person.surname(),
            Personal_Address=address.address(),
            city=address.city(),
            Country=address.country(),
            personal_email=person.email(),
            timestamp=pendulum.now().isoformat(),
            nonsense_column=random.randint(0, 1000),
        )
        for i in range(1, num_rows + 1)
    ]
    # return products rows
    if type == "products":
        logger.info(f"Generating {num_rows} products rows")
        return [
        Row(
            id=i,
            product_name=generic.text.word(),
            price=round(random.uniform(10, 500), 2),
            description=generic.text.text(quantity=1),
            stock=random.randint(0, 1000),
            company_name=finance.company(),
            timestamp=pendulum.now().isoformat(),
            nonsense_column=random.randint(0, 1000),

        )
        for i in range(1, num_rows + 1)
    ]
    # return orders rows
    logger.info(f"Generating {num_rows} orders rows")   
    return [
        Row(
            id=i,
            productid=random.randint(1, num_rows + 1),
            price=round(random.uniform(10, 500), 2),
            product_name=generic.text.word(),
            timestamp=pendulum.now().isoformat(),
            nonsense_column=random.randint(0, 1000),
        )
        for i in range(1, num_rows + 1)
    ]

# Users Data

def generate_users_frame(
    num_users: int = NUM_USERS,
    locale: str = LOCALE
) -> DataFrame:
    """
    Generates fake users data. Columns naming is intentionally incosistent :).
    """
    users_rows = generate_list_of_rows("users", num_users, locale)
    users_df = spark.createDataFrame(users_rows)
    return users_df


# Products Data
def generate_products_data(num_products: int = NUM_PRODUCTS, locale: str = LOCALE) -> DataFrame:
    """
    Generates fake products data. Columns naming is intentionally incosistent.
    """
    products_rows = generate_list_of_rows("products", num_products, locale)
    products_df = spark.createDataFrame(products_rows)
    return products_df


# Orders Data
def generate_orders_data(
    num_orders: int = NUM_ORDERS, num_products: int = NUM_PRODUCTS, locale: str = LOCALE
) -> DataFrame:
    """
    Generates fake orders data. 
    """
    orders_rows = generate_list_of_rows("orders", num_orders, locale)
    orders_df = spark.createDataFrame(orders_rows)
    return orders_df
    
class FrameConfig(NamedTuple):
    """
    frame configuration object
    """
    name: str
    df: DataFrame

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

