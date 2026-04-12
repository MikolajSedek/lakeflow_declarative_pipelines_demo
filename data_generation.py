"""
Pure data generation helpers for the fake data pipeline.

These functions have no SparkSession dependency and can be unit-tested without
a running Spark cluster.

Note on parallelism: ``generate_list_of_rows`` is CPU-bound (mimesis provider
calls are pure Python).  ``ThreadPoolExecutor`` was benchmarked up to 325 000
rows and is consistently ~5-15 % **slower** than sequential execution due to
GIL contention and thread-management overhead.  ``ProcessPoolExecutor`` avoids
the GIL but adds Row-serialisation cost that negates any gain at the current
scale.  Keep generation sequential unless profiling on production volumes
proves otherwise.
"""

from __future__ import annotations

import random
from typing import NamedTuple

import pendulum
from loguru import logger
from mimesis import Address, Finance, Generic, Person
from mimesis.enums import Locale
from pyspark.sql import DataFrame, Row

_VALID_ROW_TYPES = frozenset({"users", "products", "orders"})

DEFAULT_LOCALE = Locale.EN


def generate_list_of_rows(
    row_type: str,
    num_rows: int,
    locale: Locale = DEFAULT_LOCALE,
) -> list[Row]:
    """Generate a list of rows for a given type (users, products, or orders).

    Args:
        row_type: One of "users", "products", "orders".
        num_rows: Number of rows to generate.
        locale: Mimesis Locale enum value (default ``Locale.EN``).

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
        logger.info("Generating {} users rows", num_rows)
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
        logger.info("Generating {} products rows", num_rows)
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
    logger.info("Generating {} orders rows", num_rows)
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
