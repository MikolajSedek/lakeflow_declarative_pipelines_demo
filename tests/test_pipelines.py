"""
Unit tests for Lakeflow Declarative Pipeline definitions.

These tests use the open-source pyspark.pipelines module (PySpark 4.x) to verify
that pipeline decorators correctly register datasets and flows in the graph.

Note: The actual pipeline execution (spark.readStream, cloudFiles, CDC) requires
a Databricks Runtime environment and cannot be tested locally.  These tests focus
on the *structure* and *registration* side of the pipeline code.
"""

import pytest
from pyspark.pipelines import (
    append_flow,
    create_streaming_table,
    materialized_view,
    table,
    temporary_view,
)
from pyspark.pipelines.output import (
    MaterializedView,
    StreamingTable,
    TemporaryView,
)

# ---------------------------------------------------------------------------
# Tests: dp.materialized_view decorator
# ---------------------------------------------------------------------------


def test_registers_materialized_view_output(registry) -> None:
    """Should register a MaterializedView output with the given name."""

    @materialized_view(name="my_mv")
    def my_mv():
        pass

    mv_outputs = [o for o in registry.outputs if isinstance(o, MaterializedView)]
    assert len(mv_outputs) == 1
    assert mv_outputs[0].name == "my_mv"


def test_materialized_view_registers_associated_flow(registry) -> None:
    """Should register an associated flow targeting the materialized view."""

    @materialized_view(name="mv_with_flow")
    def mv_with_flow():
        pass

    matching = [f for f in registry.flows if f.target == "mv_with_flow"]
    assert len(matching) == 1
    assert matching[0].name == "mv_with_flow"


def test_materialized_view_flow_stores_query_function(registry) -> None:
    """Should store the decorated function as a callable in the flow."""

    @materialized_view(name="mv_func")
    def mv_func():
        return "dummy"

    flow = next(f for f in registry.flows if f.target == "mv_func")
    assert callable(flow.func)


def test_materialized_view_with_comment(registry) -> None:
    """Should persist the comment metadata on the materialized view."""

    @materialized_view(name="mv_commented", comment="A test MV")
    def mv_commented():
        pass

    mv = next(o for o in registry.outputs if o.name == "mv_commented")
    assert mv.comment == "A test MV"


def test_materialized_view_infers_name_from_function(registry) -> None:
    """Should infer the view name from the function name when not specified."""

    @materialized_view
    def auto_named_mv():
        pass

    mv = next(o for o in registry.outputs if isinstance(o, MaterializedView))
    assert mv.name == "auto_named_mv"


# ---------------------------------------------------------------------------
# Tests: dp.table decorator (streaming table)
# ---------------------------------------------------------------------------


def test_registers_streaming_table_output(registry) -> None:
    """Should register a StreamingTable output with the given name."""

    @table(name="my_streaming")
    def my_streaming():
        pass

    st_outputs = [o for o in registry.outputs if isinstance(o, StreamingTable)]
    assert len(st_outputs) == 1
    assert st_outputs[0].name == "my_streaming"


def test_table_registers_associated_flow(registry) -> None:
    """Should register an associated flow targeting the streaming table."""

    @table(name="streaming_with_flow")
    def streaming_with_flow():
        pass

    matching = [f for f in registry.flows if f.target == "streaming_with_flow"]
    assert len(matching) == 1


def test_table_infers_name_from_function(registry) -> None:
    """Should infer the table name from the function name when not specified."""

    @table
    def auto_named_table():
        pass

    st = next(o for o in registry.outputs if isinstance(o, StreamingTable))
    assert st.name == "auto_named_table"


def test_table_with_partition_cols(registry) -> None:
    """Should store partition column metadata on the streaming table."""

    @table(name="partitioned", partition_cols=["date", "region"])
    def partitioned():
        pass

    st = next(o for o in registry.outputs if o.name == "partitioned")
    assert st.partition_cols == ["date", "region"]


# ---------------------------------------------------------------------------
# Tests: dp.temporary_view decorator
# ---------------------------------------------------------------------------


def test_registers_temporary_view_output(registry) -> None:
    """Should register a TemporaryView output with the given name."""

    @temporary_view(name="my_tv")
    def my_tv():
        pass

    tv_outputs = [o for o in registry.outputs if isinstance(o, TemporaryView)]
    assert len(tv_outputs) == 1
    assert tv_outputs[0].name == "my_tv"


def test_temporary_view_registers_associated_flow(registry) -> None:
    """Should register an associated flow targeting the temporary view."""

    @temporary_view(name="tv_flow")
    def tv_flow():
        pass

    matching = [f for f in registry.flows if f.target == "tv_flow"]
    assert len(matching) == 1


def test_temporary_view_infers_name(registry) -> None:
    """Should infer the view name from the function name when not specified."""

    @temporary_view
    def inferred_name_tv():
        pass

    tv = next(o for o in registry.outputs if isinstance(o, TemporaryView))
    assert tv.name == "inferred_name_tv"


# ---------------------------------------------------------------------------
# Tests: dp.create_streaming_table
# ---------------------------------------------------------------------------


def test_create_streaming_table_output(registry) -> None:
    """Should register a StreamingTable output via the imperative API."""
    create_streaming_table(name="explicit_st")

    st_outputs = [o for o in registry.outputs if isinstance(o, StreamingTable)]
    assert len(st_outputs) == 1
    assert st_outputs[0].name == "explicit_st"


def test_create_streaming_table_does_not_register_flow(registry) -> None:
    """Should not register any flow when using create_streaming_table."""
    create_streaming_table(name="no_flow_st")

    assert len(registry.flows) == 0


def test_create_streaming_table_with_comment(registry) -> None:
    """Should persist the comment metadata on the streaming table."""
    create_streaming_table(name="commented_st", comment="gold table")

    st = next(o for o in registry.outputs if o.name == "commented_st")
    assert st.comment == "gold table"


# ---------------------------------------------------------------------------
# Tests: dp.append_flow
# ---------------------------------------------------------------------------


def test_append_flow_registers_flow_targeting_existing_table(registry) -> None:
    """Should register a flow that targets a pre-existing streaming table."""
    create_streaming_table(name="target_table")

    @append_flow(target="target_table", name="my_append")
    def my_append():
        pass

    matching = [f for f in registry.flows if f.target == "target_table"]
    assert len(matching) == 1
    assert matching[0].name == "my_append"


def test_append_flow_func_is_callable(registry) -> None:
    """Should store the decorated function as a callable in the append flow."""
    create_streaming_table(name="callable_target")

    @append_flow(target="callable_target", name="callable_flow")
    def callable_flow():
        return "data"

    flow = next(f for f in registry.flows if f.name == "callable_flow")
    assert callable(flow.func)


# ---------------------------------------------------------------------------
# Tests: Multiple registrations in a single context
# ---------------------------------------------------------------------------


def test_multiple_outputs_registered(registry) -> None:
    """Should register all four outputs when different decorators are used together."""

    @materialized_view(name="mv1")
    def mv1():
        pass

    @table(name="st1")
    def st1():
        pass

    @temporary_view(name="tv1")
    def tv1():
        pass

    create_streaming_table(name="st2")

    assert len(registry.outputs) == 4
    output_names = {o.name for o in registry.outputs}
    assert output_names == {"mv1", "st1", "tv1", "st2"}


def test_output_types_are_distinct(registry) -> None:
    """Should produce exactly one output of each decorator type."""

    @materialized_view(name="check_mv")
    def check_mv():
        pass

    @table(name="check_st")
    def check_st():
        pass

    @temporary_view(name="check_tv")
    def check_tv():
        pass

    types = {type(o).__name__ for o in registry.outputs}
    assert types == {"MaterializedView", "StreamingTable", "TemporaryView"}


# ---------------------------------------------------------------------------
# Tests: decorator raises error outside registration context
# ---------------------------------------------------------------------------


def test_materialized_view_outside_context_raises() -> None:
    """Should raise PySparkRuntimeError when used outside a registration context."""
    from pyspark.errors.exceptions.base import PySparkRuntimeError

    with pytest.raises(PySparkRuntimeError):

        @materialized_view(name="should_fail")
        def should_fail():
            pass


def test_table_outside_context_raises() -> None:
    """Should raise PySparkRuntimeError when table is used without a context."""
    from pyspark.errors.exceptions.base import PySparkRuntimeError

    with pytest.raises(PySparkRuntimeError):

        @table(name="also_fails")
        def also_fails():
            pass


def test_create_streaming_table_outside_context_raises() -> None:
    """Should raise PySparkRuntimeError for create_streaming_table without context."""
    from pyspark.errors.exceptions.base import PySparkRuntimeError

    with pytest.raises(PySparkRuntimeError):
        create_streaming_table(name="no_context")


# ---------------------------------------------------------------------------
# Tests: TablePipelineConfig from advanced pipeline
# ---------------------------------------------------------------------------


def test_table_pipeline_config_default_values() -> None:
    """Should populate all fields with sensible defaults for a given table name."""
    from pydantic.dataclasses import dataclass

    @dataclass(frozen=True)
    class TablePipelineConfig:
        table_name: str
        root_source_path: str = "/Volumes/test_catalog/test_schema/test_volume/fake_source/"
        target_catalog: str = "test_catalog"
        bronze_schema: str = "test_bronze_schema"
        silver_schema: str = "test_silver_schema"
        gold_schema: str = "test_gold_schema"
        prime_key_columns: tuple[str, ...] = ("id",)
        timestamp_column: str = "timestamp"

    config = TablePipelineConfig(table_name="fake_orders")
    assert config.table_name == "fake_orders"
    assert config.target_catalog == "test_catalog"
    assert config.bronze_schema == "test_bronze_schema"
    assert config.silver_schema == "test_silver_schema"
    assert config.gold_schema == "test_gold_schema"
    assert config.prime_key_columns == ("id",)
    assert config.timestamp_column == "timestamp"


def test_table_pipeline_config_custom_values() -> None:
    """Should accept and store custom overrides for every field."""
    from pydantic.dataclasses import dataclass

    @dataclass(frozen=True)
    class TablePipelineConfig:
        table_name: str
        root_source_path: str = "/Volumes/test_catalog/test_schema/test_volume/fake_source/"
        target_catalog: str = "test_catalog"
        bronze_schema: str = "test_bronze_schema"
        silver_schema: str = "test_silver_schema"
        gold_schema: str = "test_gold_schema"
        prime_key_columns: tuple[str, ...] = ("id",)
        timestamp_column: str = "timestamp"

    config = TablePipelineConfig(
        table_name="orders",
        target_catalog="prod_catalog",
        prime_key_columns=("order_id", "customer_id"),
        timestamp_column="updated_at",
    )
    assert config.table_name == "orders"
    assert config.target_catalog == "prod_catalog"
    assert config.prime_key_columns == ("order_id", "customer_id")
    assert config.timestamp_column == "updated_at"


def test_table_pipeline_config_is_frozen() -> None:
    """Should reject attribute mutation since the dataclass is frozen."""
    from pydantic.dataclasses import dataclass

    @dataclass(frozen=True)
    class TablePipelineConfig:
        table_name: str
        target_catalog: str = "test_catalog"

    config = TablePipelineConfig(table_name="test")
    with pytest.raises((AttributeError, TypeError)):
        config.table_name = "other"


# ---------------------------------------------------------------------------
# Tests: Pipeline name generation logic
# ---------------------------------------------------------------------------


def test_bronze_table_name() -> None:
    """Should generate a fully-qualified bronze table name with '_raw' suffix."""
    catalog = "test_catalog"
    schema = "test_bronze_schema"
    table_name = "fake_orders"
    expected = "test_catalog.test_bronze_schema.fake_orders_raw"
    assert f"{catalog}.{schema}.{table_name}_raw" == expected


def test_silver_table_name() -> None:
    """Should generate a fully-qualified silver table name with '_staging' suffix."""
    catalog = "test_catalog"
    schema = "test_silver_schema"
    table_name = "fake_orders"
    expected = "test_catalog.test_silver_schema.fake_orders_staging"
    assert f"{catalog}.{schema}.{table_name}_staging" == expected


def test_gold_table_name() -> None:
    """Should generate a fully-qualified gold table name with '_clean' suffix."""
    catalog = "test_catalog"
    schema = "test_gold_schema"
    table_name = "fake_orders"
    expected = "test_catalog.test_gold_schema.fake_orders_clean"
    assert f"{catalog}.{schema}.{table_name}_clean" == expected


def test_simple_table_name() -> None:
    """Should generate a fully-qualified simple table name with '_simple_table' suffix."""
    catalog = "test_catalog"
    schema = "test_bronze_schema"
    table_name = "fake_users"
    expected = "test_catalog.test_bronze_schema.fake_users_simple_table"
    assert f"{catalog}.{schema}.{table_name}_simple_table" == expected


def test_aggregate_gold_table_name() -> None:
    """Should generate the correct aggregate gold summary table path."""
    catalog = "test_catalog"
    schema = "test_gold_schema"
    agg_name = "summary_statistics_gold"
    expected = "test_catalog.test_gold_schema.summary_statistics_gold"
    assert f"{catalog}.{schema}.{agg_name}" == expected


def test_gold_table_paths_list() -> None:
    """Should build a list of gold table paths for all configured tables."""
    tables = ["fake_orders", "fake_products", "fake_users"]
    catalog = "test_catalog"
    schema = "test_gold_schema"
    postfix = "_clean"
    paths = [f"{catalog}.{schema}.{t}{postfix}" for t in tables]
    assert len(paths) == 3
    assert paths[0] == "test_catalog.test_gold_schema.fake_orders_clean"
    assert paths[2] == "test_catalog.test_gold_schema.fake_users_clean"
