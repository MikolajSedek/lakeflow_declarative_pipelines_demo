"""
Pure DDL SQL-builder helpers for Databricks catalog provisioning.

These functions build DDL strings without executing them, making them
fully testable without a live SparkSession.

SQL-injection protection
------------------------
Databricks identifier names (catalog, schema, volume) are validated against
``_IDENTIFIER_RE`` before being embedded in SQL.  The regex restricts names to
ASCII letters, digits, and underscores (starting with a letter or underscore),
which is the safe subset of Databricks unquoted-identifier rules.  Any name
that does not match raises ``ValueError`` immediately so no untrusted input can
reach the SQL string.  Backtick-quoting is applied after validation as a
defence-in-depth measure.
"""

from __future__ import annotations

import re

__all__ = [
    "build_create_catalog_sql",
    "build_create_schema_sql",
    "build_create_volume_sql",
    "build_setup_ddl_statements",
]

# Identifiers must start with a letter or underscore, followed by letters,
# digits, or underscores.  This covers the safe subset of Databricks names and
# rejects any attempt to inject SQL via special characters or whitespace.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_identifier(name: str) -> str:
    """Validate *name* and return it wrapped in backticks.

    Args:
        name: Databricks identifier (catalog, schema, or volume name).

    Returns:
        The identifier wrapped in backticks, e.g. ``test_catalog`` becomes
        ```test_catalog```.

    Raises:
        ValueError: If *name* is empty or contains characters outside the
            allowed set ``[A-Za-z0-9_]`` with a leading ``[A-Za-z_]``.
    """
    if not name:
        raise ValueError("Identifier must not be empty")
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid identifier {name!r}: must match [A-Za-z_][A-Za-z0-9_]*"
            " to prevent SQL injection"
        )
    return f"`{name}`"


def build_create_catalog_sql(catalog: str) -> str:
    """Return a ``CREATE CATALOG IF NOT EXISTS`` DDL string for *catalog*.

    Args:
        catalog: Databricks catalog name.

    Returns:
        A parameterised DDL string ready for ``spark.sql()``.

    Raises:
        ValueError: If *catalog* contains characters that could enable SQL injection.
    """
    return f"CREATE CATALOG IF NOT EXISTS {_quote_identifier(catalog)}"


def build_create_schema_sql(catalog: str, schema: str) -> str:
    """Return a ``CREATE SCHEMA IF NOT EXISTS`` DDL string.

    Args:
        catalog: Databricks catalog name.
        schema: Schema (database) name within *catalog*.

    Returns:
        A parameterised DDL string ready for ``spark.sql()``.

    Raises:
        ValueError: If *catalog* or *schema* contains injection-prone characters.
    """
    return (
        f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(catalog)}.{_quote_identifier(schema)}"
    )


def build_create_volume_sql(catalog: str, schema: str, volume: str) -> str:
    """Return a ``CREATE VOLUME IF NOT EXISTS`` DDL string.

    Args:
        catalog: Databricks catalog name.
        schema: Schema (database) name within *catalog*.
        volume: Volume name within *schema*.

    Returns:
        A parameterised DDL string ready for ``spark.sql()``.

    Raises:
        ValueError: If any identifier contains injection-prone characters.
    """
    return (
        f"CREATE VOLUME IF NOT EXISTS "
        f"{_quote_identifier(catalog)}.{_quote_identifier(schema)}.{_quote_identifier(volume)}"
    )


def build_setup_ddl_statements(
    catalog: str,
    schema: str,
    volume: str,
    extra_schemas: tuple[str, ...],
) -> tuple[str, ...]:
    """Build the complete set of DDL statements needed to provision the demo environment.

    This is a pure function: no side effects, no I/O.  Pass the result to a
    side-effect runner (e.g. ``run_setup_ddl`` in the notebook) to execute the
    statements against a live SparkSession.

    Args:
        catalog: Top-level catalog name.
        schema: Primary schema (used for the volume).
        volume: Volume name within *schema*.
        extra_schemas: Additional schemas to create within *catalog*.

    Returns:
        A tuple of DDL strings in dependency order (catalog first, then primary
        schema and volume, then extra schemas).

    Raises:
        ValueError: If any identifier contains injection-prone characters.
    """
    return (
        build_create_catalog_sql(catalog),
        build_create_schema_sql(catalog, schema),
        build_create_volume_sql(catalog, schema, volume),
        *[build_create_schema_sql(catalog, s) for s in extra_schemas],
    )
