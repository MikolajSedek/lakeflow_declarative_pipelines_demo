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

__all__ = ["FrameConfig", "generate_list_of_rows"]


def generate_list_of_rows(
    row_type: str,
    num_rows: int,
    locale: Locale = DEFAULT_LOCALE,
    *,
    num_users: int | None = None,
    num_products: int | None = None,
) -> list[Row]:
    """Generate a list of rows for a given type (users, products, or orders).

    Args:
        row_type: One of "users", "products", "orders".
        num_rows: Number of rows to generate.  Must be non-negative.
        locale: Mimesis Locale enum value (default ``Locale.EN``).
        num_users: Total number of users — **required** when *row_type* is
            ``"orders"`` and *num_rows* > 0.  Constrains ``userid`` to ``[1,
            num_users]`` so every foreign key maps to an existing user.
        num_products: Total number of products — **required** when *row_type*
            is ``"orders"`` and *num_rows* > 0.  Constrains ``productid`` to
            ``[1, num_products]`` so every foreign key maps to an existing
            product.

    Returns:
        A list of PySpark Row objects.

    Raises:
        ValueError: If row_type is not one of the supported types.
        ValueError: If num_rows is negative.
        ValueError: If generating orders with *num_rows* > 0 and *num_users*
            or *num_products* is ``None``, zero, or negative (would produce
            foreign keys that cannot be joined).
    """
    if num_rows < 0:
        raise ValueError(f"num_rows must be non-negative, got {num_rows}")
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

    # return orders rows – foreign keys reference valid user/product IDs
    # Both num_users and num_products are *required* for orders so that every
    # generated userid / productid is guaranteed to exist in the corresponding
    # dimension table.  Removing the fallback eliminates a silent trap where
    # FK ranges could exceed the dimension table and produce empty joins.
    if num_rows > 0 and num_users is None:
        raise ValueError(
            "num_users is required when generating orders (ensures userid foreign keys"
            " reference valid user IDs and joins are never empty)"
        )
    if num_rows > 0 and num_products is None:
        raise ValueError(
            "num_products is required when generating orders (ensures productid foreign"
            " keys reference valid product IDs and joins are never empty)"
        )
    if num_rows > 0 and (num_users is not None and num_users <= 0):
        raise ValueError(f"num_users must be positive when generating orders, got {num_users}")
    if num_rows > 0 and (num_products is not None and num_products <= 0):
        raise ValueError(
            f"num_products must be positive when generating orders, got {num_products}"
        )

    max_userid = num_users if num_users is not None else 1
    max_productid = num_products if num_products is not None else 1
    now = pendulum.now()

    logger.info("Generating {} orders rows", num_rows)
    return [
        Row(
            id=i,
            userid=random.randint(1, max_userid),  # nosec B311
            productid=random.randint(1, max_productid),  # nosec B311
            price=round(random.uniform(10, 500), 2),  # nosec B311
            product_name=generic.text.word(),
            timestamp=now.subtract(seconds=random.randint(0, 2_592_000)).isoformat(),  # nosec B311
            nonsense_column=random.randint(0, 1000),  # nosec B311
        )
        for i in range(1, num_rows + 1)
    ]


class FrameConfig(NamedTuple):
    """Frame configuration object."""

    name: str
    df: DataFrame
