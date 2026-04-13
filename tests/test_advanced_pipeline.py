"""Unit tests for the advanced medallion pipeline (03.advanced_pipeline.py).

Tests verify that the bronze/silver/gold pipeline functions register the
correct outputs and flows, and that the Databricks-only ``create_auto_cdc_flow``
is called with the right arguments.  No Databricks Runtime or live SparkSession
is required - inner query functions are registered but never executed here.
"""

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyspark.pipelines as dp_mod
import pytest
from pyspark.pipelines.output import MaterializedView, StreamingTable

_PIPELINE_PATH = Path(__file__).parent.parent / "03.advanced_pipeline.py"


def _load_advanced_pipeline_module():
    """Load the advanced pipeline module directly from its file path."""
    spec = importlib.util.spec_from_file_location("advanced_pipeline", _PIPELINE_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_ADVANCED_PIPELINE = _load_advanced_pipeline_module()

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_cdc():
    """Patch create_auto_cdc_flow (Databricks-only) onto the pyspark.pipelines module."""
    with patch.object(dp_mod, "create_auto_cdc_flow", MagicMock(), create=True) as m:
        yield m


# ---------------------------------------------------------------------------
# Tests: TablePipelineConfig (imported from the real module)
# ---------------------------------------------------------------------------


def test_table_pipeline_config_real_default_values() -> None:
    """Should return the exact defaults defined in the production module."""
    config = _ADVANCED_PIPELINE.TablePipelineConfig(table_name="fake_orders")
    assert config.table_name == "fake_orders"
    assert config.target_catalog == "test_catalog"
    assert config.bronze_schema == "test_bronze_schema"
    assert config.silver_schema == "test_silver_schema"
    assert config.gold_schema == "test_gold_schema"
    assert config.prime_key_columns == ("id",)
    assert config.timestamp_column == "timestamp"


def test_table_pipeline_config_real_is_frozen() -> None:
    """Should reject mutation since the production dataclass is frozen."""
    config = _ADVANCED_PIPELINE.TablePipelineConfig(table_name="fake_orders")
    with pytest.raises((AttributeError, TypeError)):
        config.table_name = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Tests: create_raw_bronze_table
# ---------------------------------------------------------------------------


def test_create_raw_bronze_table_registers_streaming_table(registry) -> None:
    """Should register a StreamingTable output with the given bronze table name."""
    _ADVANCED_PIPELINE.create_raw_bronze_table(
        "test_catalog.test_bronze_schema.fake_orders_raw",
        "/data/source/fake_orders",
    )

    st_outputs = [o for o in registry.outputs if isinstance(o, StreamingTable)]
    assert len(st_outputs) == 1
    assert st_outputs[0].name == "test_catalog.test_bronze_schema.fake_orders_raw"


def test_create_raw_bronze_table_registers_associated_flow(registry) -> None:
    """Should register an associated flow targeting the bronze streaming table."""
    _ADVANCED_PIPELINE.create_raw_bronze_table(
        "test_catalog.test_bronze_schema.fake_orders_raw",
        "/data/source/fake_orders",
    )

    flows = [
        f for f in registry.flows if f.target == "test_catalog.test_bronze_schema.fake_orders_raw"
    ]
    assert len(flows) == 1


def test_create_raw_bronze_table_has_non_empty_comment(registry) -> None:
    """Should attach a non-empty descriptive comment to the bronze streaming table."""
    _ADVANCED_PIPELINE.create_raw_bronze_table(
        "test_catalog.test_bronze_schema.fake_orders_raw",
        "/data/source/fake_orders",
    )

    st = next(o for o in registry.outputs if isinstance(o, StreamingTable))
    assert st.comment is not None
    assert len(st.comment) > 0


# ---------------------------------------------------------------------------
# Tests: create_silver_staging_table
# ---------------------------------------------------------------------------


def test_create_silver_staging_table_registers_streaming_table(registry) -> None:
    """Should register a StreamingTable output with the given silver table name."""
    _ADVANCED_PIPELINE.create_silver_staging_table(
        "test_catalog.test_silver_schema.fake_orders_staging",
        "test_catalog.test_bronze_schema.fake_orders_raw",
    )

    st_outputs = [o for o in registry.outputs if isinstance(o, StreamingTable)]
    assert len(st_outputs) == 1
    assert st_outputs[0].name == "test_catalog.test_silver_schema.fake_orders_staging"


def test_create_silver_staging_table_registers_associated_flow(registry) -> None:
    """Should register an associated flow targeting the silver streaming table."""
    _ADVANCED_PIPELINE.create_silver_staging_table(
        "test_catalog.test_silver_schema.fake_orders_staging",
        "test_catalog.test_bronze_schema.fake_orders_raw",
    )

    flows = [
        f
        for f in registry.flows
        if f.target == "test_catalog.test_silver_schema.fake_orders_staging"
    ]
    assert len(flows) == 1


def test_create_silver_staging_table_has_non_empty_comment(registry) -> None:
    """Should attach a non-empty descriptive comment to the silver streaming table."""
    _ADVANCED_PIPELINE.create_silver_staging_table(
        "test_catalog.test_silver_schema.fake_orders_staging",
        "test_catalog.test_bronze_schema.fake_orders_raw",
    )

    st = next(o for o in registry.outputs if isinstance(o, StreamingTable))
    assert st.comment is not None
    assert len(st.comment) > 0


# ---------------------------------------------------------------------------
# Tests: create_gold_merged_table
# ---------------------------------------------------------------------------


def test_create_gold_merged_table_registers_streaming_table(registry, mock_cdc) -> None:
    """Should register a StreamingTable output for the gold table."""
    _ADVANCED_PIPELINE.create_gold_merged_table(
        silver_table_name="test_catalog.test_silver_schema.fake_orders_staging",
        gold_table_name="test_catalog.test_gold_schema.fake_orders_clean",
    )

    st_outputs = [o for o in registry.outputs if isinstance(o, StreamingTable)]
    assert len(st_outputs) == 1
    assert st_outputs[0].name == "test_catalog.test_gold_schema.fake_orders_clean"


def test_create_gold_merged_table_calls_cdc_once(registry, mock_cdc) -> None:
    """Should invoke create_auto_cdc_flow exactly once."""
    _ADVANCED_PIPELINE.create_gold_merged_table(
        silver_table_name="test_catalog.test_silver_schema.fake_orders_staging",
        gold_table_name="test_catalog.test_gold_schema.fake_orders_clean",
    )

    assert mock_cdc.call_count == 1


def test_create_gold_merged_table_cdc_source_is_silver(registry, mock_cdc) -> None:
    """Should pass the silver table as the CDC source."""
    _ADVANCED_PIPELINE.create_gold_merged_table(
        silver_table_name="test_catalog.test_silver_schema.fake_orders_staging",
        gold_table_name="test_catalog.test_gold_schema.fake_orders_clean",
    )

    assert (
        mock_cdc.call_args.kwargs["source"] == "test_catalog.test_silver_schema.fake_orders_staging"
    )


def test_create_gold_merged_table_cdc_target_is_gold(registry, mock_cdc) -> None:
    """Should pass the gold table name as the CDC target."""
    _ADVANCED_PIPELINE.create_gold_merged_table(
        silver_table_name="test_catalog.test_silver_schema.fake_orders_staging",
        gold_table_name="test_catalog.test_gold_schema.fake_orders_clean",
    )

    assert mock_cdc.call_args.kwargs["target"] == "test_catalog.test_gold_schema.fake_orders_clean"


def test_create_gold_merged_table_cdc_uses_default_key(registry, mock_cdc) -> None:
    """Should use ('id',) as the default primary key for the CDC flow."""
    _ADVANCED_PIPELINE.create_gold_merged_table(
        silver_table_name="test_catalog.test_silver_schema.fake_orders_staging",
        gold_table_name="test_catalog.test_gold_schema.fake_orders_clean",
    )

    assert mock_cdc.call_args.kwargs["keys"] == ["id"]


def test_create_gold_merged_table_cdc_uses_default_timestamp(registry, mock_cdc) -> None:
    """Should use 'timestamp' as the default sequence column for the CDC flow."""
    _ADVANCED_PIPELINE.create_gold_merged_table(
        silver_table_name="test_catalog.test_silver_schema.fake_orders_staging",
        gold_table_name="test_catalog.test_gold_schema.fake_orders_clean",
    )

    assert mock_cdc.call_args.kwargs["sequence_by"] == "timestamp"


def test_create_gold_merged_table_has_non_empty_comment(registry, mock_cdc) -> None:
    """Should attach a non-empty descriptive comment to the gold streaming table."""
    _ADVANCED_PIPELINE.create_gold_merged_table(
        silver_table_name="test_catalog.test_silver_schema.fake_orders_staging",
        gold_table_name="test_catalog.test_gold_schema.fake_orders_clean",
    )

    st = next(o for o in registry.outputs if isinstance(o, StreamingTable))
    assert st.comment is not None
    assert len(st.comment) > 0


def test_create_gold_merged_table_cdc_custom_keys(registry, mock_cdc) -> None:
    """Should forward custom primary key columns to the CDC flow."""
    _ADVANCED_PIPELINE.create_gold_merged_table(
        silver_table_name="test_catalog.test_silver_schema.fake_orders_staging",
        gold_table_name="test_catalog.test_gold_schema.fake_orders_clean",
        prime_key_columns=("order_id", "customer_id"),
    )

    assert mock_cdc.call_args.kwargs["keys"] == ["order_id", "customer_id"]


# ---------------------------------------------------------------------------
# Tests: run_single_pipeline
# ---------------------------------------------------------------------------


def test_run_single_pipeline_creates_three_streaming_tables(registry, mock_cdc) -> None:
    """Should register exactly three StreamingTable outputs (bronze, silver, gold)."""
    config = _ADVANCED_PIPELINE.TablePipelineConfig(table_name="fake_orders")
    _ADVANCED_PIPELINE.run_single_pipeline(config)

    st_outputs = [o for o in registry.outputs if isinstance(o, StreamingTable)]
    assert len(st_outputs) == 3


def test_run_single_pipeline_calls_cdc_once(registry, mock_cdc) -> None:
    """Should invoke CDC flow exactly once per pipeline run."""
    config = _ADVANCED_PIPELINE.TablePipelineConfig(table_name="fake_orders")
    _ADVANCED_PIPELINE.run_single_pipeline(config)

    assert mock_cdc.call_count == 1


def test_run_single_pipeline_registers_bronze_silver_gold_names(registry, mock_cdc) -> None:
    """Should register streaming tables with the expected bronze, silver, and gold names."""
    config = _ADVANCED_PIPELINE.TablePipelineConfig(table_name="fake_orders")
    _ADVANCED_PIPELINE.run_single_pipeline(config)

    names = {o.name for o in registry.outputs}
    assert "test_catalog.test_bronze_schema.fake_orders_raw" in names
    assert "test_catalog.test_silver_schema.fake_orders_staging" in names
    assert "test_catalog.test_gold_schema.fake_orders_clean" in names


# ---------------------------------------------------------------------------
# Tests: run_all_pipelines
# ---------------------------------------------------------------------------


def test_run_all_pipelines_creates_outputs_for_all_tables(registry, mock_cdc) -> None:
    """Should create 3 streaming tables per table for all 3 configured tables (9 total)."""
    _ADVANCED_PIPELINE.run_all_pipelines()

    st_outputs = [o for o in registry.outputs if isinstance(o, StreamingTable)]
    assert len(st_outputs) == 9  # 3 tables × 3 layers (bronze, silver, gold)


def test_run_all_pipelines_calls_cdc_once_per_table(registry, mock_cdc) -> None:
    """Should invoke create_auto_cdc_flow exactly once for each of the 3 tables."""
    _ADVANCED_PIPELINE.run_all_pipelines()

    assert mock_cdc.call_count == 3


# ---------------------------------------------------------------------------
# Tests: aggregate_gold_tables
# ---------------------------------------------------------------------------


def test_aggregate_gold_tables_registers_materialized_view(registry) -> None:
    """Should register exactly one MaterializedView output."""
    _ADVANCED_PIPELINE.aggregate_gold_tables()

    mv_outputs = [o for o in registry.outputs if isinstance(o, MaterializedView)]
    assert len(mv_outputs) == 1


def test_aggregate_gold_tables_mv_name_is_fully_qualified(registry) -> None:
    """Should register the materialized view with the expected fully-qualified name."""
    _ADVANCED_PIPELINE.aggregate_gold_tables()

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    assert mv.name == "test_catalog.test_gold_schema.summary_statistics_gold"


def test_aggregate_gold_tables_empty_tables_is_noop(registry) -> None:
    """Should register no outputs when given an empty tables_names tuple."""
    _ADVANCED_PIPELINE.aggregate_gold_tables(tables_names=())

    assert len(registry.outputs) == 0


def test_aggregate_gold_tables_registers_associated_flow(registry) -> None:
    """Should register an associated flow targeting the summary statistics view."""
    _ADVANCED_PIPELINE.aggregate_gold_tables()

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    flows = [f for f in registry.flows if f.target == mv.name]
    assert len(flows) == 1


def test_aggregate_gold_tables_mv_has_non_empty_comment(registry) -> None:
    """Should attach a non-empty descriptive comment to the aggregate gold materialized view."""
    _ADVANCED_PIPELINE.aggregate_gold_tables()

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    assert mv.comment is not None
    assert len(mv.comment) > 0


# ---------------------------------------------------------------------------
# Tests: create_gold_revenue_per_product
# ---------------------------------------------------------------------------


def test_revenue_per_product_registers_materialized_view(registry) -> None:
    """Should register exactly one MaterializedView output for revenue per product."""
    _ADVANCED_PIPELINE.create_gold_revenue_per_product()

    mv_outputs = [o for o in registry.outputs if isinstance(o, MaterializedView)]
    assert len(mv_outputs) == 1


def test_revenue_per_product_mv_name_is_fully_qualified(registry) -> None:
    """Should register the view with the expected fully-qualified name."""
    _ADVANCED_PIPELINE.create_gold_revenue_per_product()

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    assert mv.name == "test_catalog.test_gold_schema.revenue_per_product_gold"


def test_revenue_per_product_has_non_empty_comment(registry) -> None:
    """Should attach a non-empty descriptive comment to the revenue view."""
    _ADVANCED_PIPELINE.create_gold_revenue_per_product()

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    assert mv.comment is not None
    assert len(mv.comment) > 0


def test_revenue_per_product_registers_associated_flow(registry) -> None:
    """Should register an associated flow targeting the revenue per product view."""
    _ADVANCED_PIPELINE.create_gold_revenue_per_product()

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    flows = [f for f in registry.flows if f.target == mv.name]
    assert len(flows) == 1


# ---------------------------------------------------------------------------
# Tests: create_gold_customer_order_summary
# ---------------------------------------------------------------------------


def test_customer_order_summary_registers_materialized_view(registry) -> None:
    """Should register exactly one MaterializedView output for customer order summary."""
    _ADVANCED_PIPELINE.create_gold_customer_order_summary()

    mv_outputs = [o for o in registry.outputs if isinstance(o, MaterializedView)]
    assert len(mv_outputs) == 1


def test_customer_order_summary_mv_name_is_fully_qualified(registry) -> None:
    """Should register the view with the expected fully-qualified name."""
    _ADVANCED_PIPELINE.create_gold_customer_order_summary()

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    assert mv.name == "test_catalog.test_gold_schema.customer_order_summary_gold"


def test_customer_order_summary_has_non_empty_comment(registry) -> None:
    """Should attach a non-empty descriptive comment to the customer summary view."""
    _ADVANCED_PIPELINE.create_gold_customer_order_summary()

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    assert mv.comment is not None
    assert len(mv.comment) > 0


def test_customer_order_summary_registers_associated_flow(registry) -> None:
    """Should register an associated flow targeting the customer order summary view."""
    _ADVANCED_PIPELINE.create_gold_customer_order_summary()

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    flows = [f for f in registry.flows if f.target == mv.name]
    assert len(flows) == 1


# ---------------------------------------------------------------------------
# Tests: create_gold_orders_enriched
# ---------------------------------------------------------------------------


def test_orders_enriched_registers_materialized_view(registry) -> None:
    """Should register exactly one MaterializedView output for enriched orders."""
    _ADVANCED_PIPELINE.create_gold_orders_enriched()

    mv_outputs = [o for o in registry.outputs if isinstance(o, MaterializedView)]
    assert len(mv_outputs) == 1


def test_orders_enriched_mv_name_is_fully_qualified(registry) -> None:
    """Should register the view with the expected fully-qualified name."""
    _ADVANCED_PIPELINE.create_gold_orders_enriched()

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    assert mv.name == "test_catalog.test_gold_schema.orders_enriched_gold"


def test_orders_enriched_has_non_empty_comment(registry) -> None:
    """Should attach a non-empty descriptive comment to the enriched orders view."""
    _ADVANCED_PIPELINE.create_gold_orders_enriched()

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    assert mv.comment is not None
    assert len(mv.comment) > 0


def test_orders_enriched_registers_associated_flow(registry) -> None:
    """Should register an associated flow targeting the enriched orders view."""
    _ADVANCED_PIPELINE.create_gold_orders_enriched()

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    flows = [f for f in registry.flows if f.target == mv.name]
    assert len(flows) == 1


# ---------------------------------------------------------------------------
# Tests: create_all_gold_kpi_tables
# ---------------------------------------------------------------------------


def test_create_all_gold_kpi_tables_registers_three_materialized_views(registry) -> None:
    """Should register exactly three MaterializedView outputs for all KPI tables."""
    _ADVANCED_PIPELINE.create_all_gold_kpi_tables()

    mv_outputs = [o for o in registry.outputs if isinstance(o, MaterializedView)]
    assert len(mv_outputs) == 3


def test_create_all_gold_kpi_tables_names_are_unique(registry) -> None:
    """Should register three materialized views with unique names."""
    _ADVANCED_PIPELINE.create_all_gold_kpi_tables()

    mv_names = {o.name for o in registry.outputs if isinstance(o, MaterializedView)}
    assert len(mv_names) == 3
    assert "test_catalog.test_gold_schema.revenue_per_product_gold" in mv_names
    assert "test_catalog.test_gold_schema.customer_order_summary_gold" in mv_names
    assert "test_catalog.test_gold_schema.orders_enriched_gold" in mv_names
