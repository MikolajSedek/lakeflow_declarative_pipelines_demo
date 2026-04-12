"""
Pure data generation helpers for the fake data pipeline.

These functions have no SparkSession dependency and can be unit-tested without
a running Spark cluster.
"""

import random
from typing import NamedTuple

import pendulum
from loguru import logger
from mimesis import Address, Finance, Generic, Person
from pyspark.sql import DataFrame, Row

LOCALE = "en"

_VALID_ROW_TYPES = {"users", "products", "orders"}


def generate_list_of_rows(
    row_type: str,
    num_rows: int,
    locale: str = LOCALE,
) -> list[Row]:
    """
    Generates a list of rows for a given type (users, products, or orders).

    Args:
        row_type: One of "users", "products", "orders".
        num_rows: Number of rows to generate.
        locale: Mimesis locale string (default "en").

    Returns:
        A list of PySpark Row objects.

    Raises:
        ValueError: If row_type is not one of the supported types.
    """
    if row_type not in _VALID_ROW_TYPES:
        raise ValueError(
            f"Invalid row_type: {row_type!r}. Must be one of {sorted(_VALID_ROW_TYPES)}"
        )

    person = Person(locale)
    address = Address(locale)
    generic = Generic(locale)
    finance = Finance(locale)

    # return users rows
    if row_type == "users":
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
                nonsense_column=random.randint(0, 1000),  # nosec B311
            )
            for i in range(1, num_rows + 1)
        ]

    # return products rows
    if row_type == "products":
        logger.info(f"Generating {num_rows} products rows")
        return [
            Row(
                id=i,
                product_name=generic.text.word(),
                price=round(random.uniform(10, 500), 2),  # nosec B311
                description=generic.text.text(quantity=1),
                stock=random.randint(0, 1000),  # nosec B311
                company_name=finance.company(),
                timestamp=pendulum.now().isoformat(),
                nonsense_column=random.randint(0, 1000),  # nosec B311
            )
            for i in range(1, num_rows + 1)
        ]

    # return orders rows
    logger.info(f"Generating {num_rows} orders rows")
    return [
        Row(
            id=i,
            productid=random.randint(1, num_rows + 1),  # nosec B311
            price=round(random.uniform(10, 500), 2),  # nosec B311
            product_name=generic.text.word(),
            timestamp=pendulum.now().isoformat(),
            nonsense_column=random.randint(0, 1000),  # nosec B311
        )
        for i in range(1, num_rows + 1)
    ]


class FrameConfig(NamedTuple):
    """Frame configuration object."""

    name: str
    df: DataFrame
