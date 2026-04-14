"""
Unit tests for pure DDL builder functions in modules/ddl_helpers.py.

All functions under test are pure (no SparkSession dependency) and are
tested with ``@pytest.mark.parametrize`` per the functional-programming
style guide.
"""

import pytest
from modules.ddl_helpers import (
    _quote_identifier,
    build_create_catalog_sql,
    build_create_schema_sql,
    build_create_volume_sql,
    build_setup_ddl_statements,
)

# ---------------------------------------------------------------------------
# _quote_identifier — validation and quoting
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("test_catalog", "`test_catalog`"),
        ("my_schema", "`my_schema`"),
        ("volume1", "`volume1`"),
        ("CamelCase", "`CamelCase`"),
        ("_private", "`_private`"),
        ("abc123", "`abc123`"),
    ],
)
def test_quote_identifier_valid_names(name: str, expected: str) -> None:
    """Should return the name wrapped in backticks for valid identifiers."""
    assert _quote_identifier(name) == expected


@pytest.mark.parametrize(
    "invalid_name",
    [
        "",  # empty string
        "123starts_with_digit",  # starts with a digit
        "has space",  # contains whitespace
        "semi;colon",  # semicolon — classic SQL injection vector
        "'; DROP TABLE users; --",  # SQL injection string
        "test-catalog",  # hyphen not allowed
        "catalog.schema",  # dot not allowed
        "name\nwith\nnewline",  # newline
    ],
)
def test_quote_identifier_invalid_names_raise_value_error(invalid_name: str) -> None:
    """Should raise ValueError when the identifier is empty or contains unsafe characters."""
    with pytest.raises(ValueError, match=r"Invalid identifier|must not be empty"):
        _quote_identifier(invalid_name)


# ---------------------------------------------------------------------------
# build_create_catalog_sql — pure DDL builder
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("catalog", "expected"),
    [
        ("test_catalog", "CREATE CATALOG IF NOT EXISTS `test_catalog`"),
        ("prod_catalog", "CREATE CATALOG IF NOT EXISTS `prod_catalog`"),
        ("my_catalog_2", "CREATE CATALOG IF NOT EXISTS `my_catalog_2`"),
    ],
)
def test_build_create_catalog_sql_correct_ddl(catalog: str, expected: str) -> None:
    """Should produce the exact CREATE CATALOG DDL string for a valid catalog name."""
    assert build_create_catalog_sql(catalog) == expected


@pytest.mark.parametrize(
    "bad_catalog",
    [
        "",
        "bad catalog",
        "bad;catalog",
        "'; DROP CATALOG prod; --",
    ],
)
def test_build_create_catalog_sql_invalid_name_raises(bad_catalog: str) -> None:
    """Should raise ValueError when the catalog name is invalid."""
    with pytest.raises(ValueError, match=r"Invalid identifier|must not be empty"):
        build_create_catalog_sql(bad_catalog)


# ---------------------------------------------------------------------------
# build_create_schema_sql — pure DDL builder
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("catalog", "schema", "expected"),
    [
        (
            "test_catalog",
            "test_schema",
            "CREATE SCHEMA IF NOT EXISTS `test_catalog`.`test_schema`",
        ),
        (
            "test_catalog",
            "test_bronze_schema",
            "CREATE SCHEMA IF NOT EXISTS `test_catalog`.`test_bronze_schema`",
        ),
        (
            "prod_catalog",
            "gold_schema",
            "CREATE SCHEMA IF NOT EXISTS `prod_catalog`.`gold_schema`",
        ),
    ],
)
def test_build_create_schema_sql_correct_ddl(catalog: str, schema: str, expected: str) -> None:
    """Should produce the exact CREATE SCHEMA DDL string for valid catalog and schema names."""
    assert build_create_schema_sql(catalog, schema) == expected


@pytest.mark.parametrize(
    ("bad_catalog", "bad_schema"),
    [
        ("", "valid_schema"),
        ("valid_catalog", ""),
        ("bad catalog", "valid_schema"),
        ("valid_catalog", "bad;schema"),
        ("'; DROP SCHEMA prod; --", "gold"),
    ],
)
def test_build_create_schema_sql_invalid_name_raises(bad_catalog: str, bad_schema: str) -> None:
    """Should raise ValueError when either catalog or schema name is invalid."""
    with pytest.raises(ValueError, match=r"Invalid identifier|must not be empty"):
        build_create_schema_sql(bad_catalog, bad_schema)


# ---------------------------------------------------------------------------
# build_create_volume_sql — pure DDL builder
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("catalog", "schema", "volume", "expected"),
    [
        (
            "test_catalog",
            "test_schema",
            "test_volume",
            "CREATE VOLUME IF NOT EXISTS `test_catalog`.`test_schema`.`test_volume`",
        ),
        (
            "prod_catalog",
            "data_schema",
            "raw_volume",
            "CREATE VOLUME IF NOT EXISTS `prod_catalog`.`data_schema`.`raw_volume`",
        ),
    ],
)
def test_build_create_volume_sql_correct_ddl(
    catalog: str, schema: str, volume: str, expected: str
) -> None:
    """Should produce the exact CREATE VOLUME DDL string for valid three-part names."""
    assert build_create_volume_sql(catalog, schema, volume) == expected


@pytest.mark.parametrize(
    ("bad_catalog", "bad_schema", "bad_volume"),
    [
        ("", "schema", "volume"),
        ("catalog", "", "volume"),
        ("catalog", "schema", ""),
        ("catalog", "schema", "bad volume"),
        ("catalog", "schema", "vol;ume"),
    ],
)
def test_build_create_volume_sql_invalid_name_raises(
    bad_catalog: str, bad_schema: str, bad_volume: str
) -> None:
    """Should raise ValueError when any of the three identifier parts is invalid."""
    with pytest.raises(ValueError, match=r"Invalid identifier|must not be empty"):
        build_create_volume_sql(bad_catalog, bad_schema, bad_volume)


# ---------------------------------------------------------------------------
# build_setup_ddl_statements — integration of all builders
# ---------------------------------------------------------------------------


def test_build_setup_ddl_statements_returns_tuple() -> None:
    """Should return an immutable tuple of DDL strings."""
    stmts = build_setup_ddl_statements(
        "test_catalog",
        "test_schema",
        "test_volume",
        ("test_bronze_schema", "test_silver_schema", "test_gold_schema"),
    )
    assert isinstance(stmts, tuple)


def test_build_setup_ddl_statements_correct_count() -> None:
    """Should return 1 catalog + 1 primary schema + 1 volume + N extra schemas."""
    extra = ("bronze_schema", "silver_schema", "gold_schema")
    stmts = build_setup_ddl_statements("cat", "sch", "vol", extra)
    # 1 catalog + 1 schema + 1 volume + 3 extra schemas = 6
    assert len(stmts) == 6


def test_build_setup_ddl_statements_no_extra_schemas() -> None:
    """Should return exactly 3 statements when no extra schemas are requested."""
    stmts = build_setup_ddl_statements("cat", "sch", "vol", ())
    assert len(stmts) == 3


@pytest.mark.parametrize("statement_index", [0, 1, 2, 3, 4, 5])
def test_build_setup_ddl_statements_all_are_nonempty_strings(
    statement_index: int,
) -> None:
    """Every statement in the tuple should be a non-empty string."""
    stmts = build_setup_ddl_statements(
        "test_catalog",
        "test_schema",
        "test_volume",
        ("test_bronze_schema", "test_silver_schema", "test_gold_schema"),
    )
    assert isinstance(stmts[statement_index], str)
    assert len(stmts[statement_index]) > 0


def test_build_setup_ddl_statements_first_is_create_catalog() -> None:
    """The first DDL statement should be a CREATE CATALOG statement."""
    stmts = build_setup_ddl_statements("test_catalog", "sch", "vol", ())
    assert stmts[0] == "CREATE CATALOG IF NOT EXISTS `test_catalog`"


def test_build_setup_ddl_statements_volume_ddl_is_present() -> None:
    """The tuple should contain exactly one CREATE VOLUME statement."""
    stmts = build_setup_ddl_statements("cat", "sch", "vol", ())
    volume_stmts = [s for s in stmts if s.startswith("CREATE VOLUME")]
    assert len(volume_stmts) == 1


def test_build_setup_ddl_statements_extra_schemas_appear_last() -> None:
    """Extra schema DDL statements should appear after catalog, schema, and volume."""
    extra = ("bronze_schema",)
    stmts = build_setup_ddl_statements("cat", "sch", "vol", extra)
    # last statement should reference the extra schema
    assert "`bronze_schema`" in stmts[-1]


def test_build_setup_ddl_statements_invalid_identifier_raises() -> None:
    """Should propagate ValueError from _quote_identifier on injection-prone input."""
    with pytest.raises(ValueError, match=r"Invalid identifier|must not be empty"):
        build_setup_ddl_statements("bad;catalog", "sch", "vol", ())


# ---------------------------------------------------------------------------
# run_setup_ddl — side-effect wrapper (tested via mock SparkSession)
# ---------------------------------------------------------------------------


def test_run_setup_ddl_calls_spark_sql_for_each_statement() -> None:
    """Should invoke spark.sql() once per DDL statement returned by build_setup_ddl_statements."""
    import importlib.util
    from pathlib import Path
    from unittest.mock import MagicMock

    nb_path = Path(__file__).parent.parent / "src/python/notebooks/01.create_fake_data.py"
    spec = importlib.util.spec_from_file_location("create_fake_data", nb_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    mock_spark = MagicMock()
    extra = (mod.BRONZE_SCHEMA, mod.SILVER_SCHEMA, mod.GOLD_SCHEMA)
    expected_count = len(build_setup_ddl_statements(mod.CATALOG, mod.SCHEMA, mod.VOLUME, extra))

    mod.run_setup_ddl(mock_spark)

    assert mock_spark.sql.call_count == expected_count


def test_run_setup_ddl_passes_correct_ddl_statements() -> None:
    """Should call spark.sql() with exactly the DDL strings from build_setup_ddl_statements."""
    import importlib.util
    from pathlib import Path
    from unittest.mock import MagicMock, call

    nb_path = Path(__file__).parent.parent / "src/python/notebooks/01.create_fake_data.py"
    spec = importlib.util.spec_from_file_location("create_fake_data", nb_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    mock_spark = MagicMock()
    extra = (mod.BRONZE_SCHEMA, mod.SILVER_SCHEMA, mod.GOLD_SCHEMA)
    expected_statements = build_setup_ddl_statements(mod.CATALOG, mod.SCHEMA, mod.VOLUME, extra)

    mod.run_setup_ddl(mock_spark)

    actual_calls = mock_spark.sql.call_args_list
    assert actual_calls == [call(stmt) for stmt in expected_statements]
