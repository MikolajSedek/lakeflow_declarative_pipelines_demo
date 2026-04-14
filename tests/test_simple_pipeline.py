"""Unit tests for the simple pipeline (02.simple_pipeline.py).

Verifies that ``create_simple_materialized_view`` and ``create_tables``
correctly register outputs and flows in the pyspark.pipelines graph element
registry without requiring a Databricks Runtime or a live SparkSession.
"""

import importlib.util
from pathlib import Path

from pyspark.pipelines.output import MaterializedView

_PIPELINE_PATH = Path(__file__).parent.parent / "src/python/notebooks/02.simple_pipeline.py"


def _load_simple_pipeline_module():
    """Load the simple pipeline module directly from its file path."""
    spec = importlib.util.spec_from_file_location("simple_pipeline", _PIPELINE_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_SIMPLE_PIPELINE = _load_simple_pipeline_module()

# ---------------------------------------------------------------------------
# Tests: create_simple_materialized_view
# ---------------------------------------------------------------------------


def test_create_simple_mv_registers_output(registry) -> None:
    """Should register a MaterializedView output with the expected fully-qualified name."""
    _SIMPLE_PIPELINE.create_simple_materialized_view("fake_orders")

    mv_outputs = [o for o in registry.outputs if isinstance(o, MaterializedView)]
    assert len(mv_outputs) == 1
    assert mv_outputs[0].name == "test_catalog.test_bronze_schema.fake_orders_simple_table"


def test_create_simple_mv_name_has_correct_suffix(registry) -> None:
    """Should append '_simple_table' suffix to the view name."""
    _SIMPLE_PIPELINE.create_simple_materialized_view("fake_products")

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    assert mv.name.endswith("_simple_table")


def test_create_simple_mv_registers_associated_flow(registry) -> None:
    """Should register an associated flow that targets the materialized view."""
    _SIMPLE_PIPELINE.create_simple_materialized_view("fake_users")

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    matching_flows = [f for f in registry.flows if f.target == mv.name]
    assert len(matching_flows) == 1


def test_create_simple_mv_flow_func_is_callable(registry) -> None:
    """Should store a callable query function on the registered flow."""
    _SIMPLE_PIPELINE.create_simple_materialized_view("fake_orders")

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    flow = next(f for f in registry.flows if f.target == mv.name)
    assert callable(flow.func)


def test_create_simple_mv_custom_catalog_and_schema(registry) -> None:
    """Should use the provided catalog and schema overrides in the view name."""
    _SIMPLE_PIPELINE.create_simple_materialized_view(
        "fake_orders",
        catalog="prod_catalog",
        schema="prod_schema",
    )

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    assert mv.name == "prod_catalog.prod_schema.fake_orders_simple_table"


def test_create_simple_mv_has_non_empty_comment(registry) -> None:
    """Should attach a non-empty descriptive comment to the materialized view."""
    _SIMPLE_PIPELINE.create_simple_materialized_view("fake_orders")

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    assert mv.comment is not None
    assert len(mv.comment) > 0


def test_create_simple_mv_comment_references_table_name(registry) -> None:
    """Should include the table name in the materialized view comment."""
    _SIMPLE_PIPELINE.create_simple_materialized_view("fake_products")

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    assert "fake_products" in mv.comment


# ---------------------------------------------------------------------------
# Tests: create_tables
# ---------------------------------------------------------------------------


def test_create_tables_registers_one_mv_per_table(registry) -> None:
    """Should register exactly one materialized view for each table in the list."""
    tables = ("fake_orders", "fake_products", "fake_users")
    _SIMPLE_PIPELINE.create_tables(tables)

    mv_outputs = [o for o in registry.outputs if isinstance(o, MaterializedView)]
    assert len(mv_outputs) == 3


def test_create_tables_mv_names_are_unique(registry) -> None:
    """Each registered materialized view should have a unique name."""
    tables = ("fake_orders", "fake_products", "fake_users")
    _SIMPLE_PIPELINE.create_tables(tables)

    names = {o.name for o in registry.outputs}
    assert len(names) == 3


def test_create_tables_empty_list_registers_no_outputs(registry) -> None:
    """Should register no outputs when given an empty table tuple."""
    _SIMPLE_PIPELINE.create_tables(())

    assert len(registry.outputs) == 0
