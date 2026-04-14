---
name: pytest-patterns
description: Concise, battle-tested pytest recipes for fixtures, parametrize, mocking, and test organisation in data-engineering projects.
---

Practical pytest patterns for Python data-engineering projects. Apply these recipes to write tests that are fast to write, easy to read, and cheap to maintain.

## Parametrize groups of similar tests

When several test functions differ only in inputs or expected outputs, replace them with a single parametrized function.  
The rule of thumb: **if you can describe all the cases in a table, use parametrize**.

```python
import pytest

# before — five separate functions
def test_suffix_raw():
    assert f"test_catalog.test_bronze.orders_raw" == "test_catalog.test_bronze.orders_raw"

def test_suffix_staging():
    assert f"test_catalog.test_silver.orders_staging" == "test_catalog.test_silver.orders_staging"

# after — one function, N cases
@pytest.mark.parametrize(
    ("catalog", "schema", "table", "suffix", "expected"),
    [
        ("test_catalog", "test_bronze", "orders", "raw",     "test_catalog.test_bronze.orders_raw"),
        ("test_catalog", "test_silver", "orders", "staging", "test_catalog.test_silver.orders_staging"),
        ("test_catalog", "test_gold",   "orders", "clean",   "test_catalog.test_gold.orders_clean"),
    ],
)
def test_qualified_table_name(catalog, schema, table, suffix, expected) -> None:
    """Should build a fully-qualified table name from its components."""
    assert f"{catalog}.{schema}.{table}_{suffix}" == expected
```

### Parametrizing constant assertions

A common pattern is verifying that a set of module-level constants hold expected values.  
Instead of one function per constant, use a single parametrized test:

```python
@pytest.mark.parametrize(
    ("attr", "expected"),
    [
        ("SOURCE_TABLE",  "catalog.silver.orders_staging"),
        ("TARGET_CATALOG", "catalog"),
        ("KEY_COLUMN",    "id"),
        ("SEQ_COLUMN",    "timestamp"),
    ],
)
def test_pipeline_constant(attr: str, expected) -> None:
    """Should define each module-level constant with its expected value."""
    assert getattr(pipeline_module, attr) == expected
```

### Parametrizing callables and expected names

When testing that several factory functions each register the correct output name:

```python
import pytest

@pytest.mark.parametrize(
    ("create_fn", "expected_name"),
    [
        (module.create_revenue_view,  "catalog.gold.revenue_gold"),
        (module.create_summary_view,  "catalog.gold.summary_gold"),
        (module.create_geography_view,"catalog.gold.geography_gold"),
    ],
)
def test_gold_view_name(registry, create_fn, expected_name: str) -> None:
    """Should register a MaterializedView with the expected fully-qualified name."""
    create_fn()
    mv = next(o for o in registry.outputs)
    assert mv.name == expected_name
```

---

## Fixtures — scoping and composition

Choose the fixture scope that matches the *cost* of creating the resource vs. the *risk* of sharing state between tests.

| Resource              | Recommended scope | Reason                                      |
|-----------------------|-------------------|---------------------------------------------|
| SparkSession          | `session`         | Expensive to start; stateless after creation |
| DataFrame (read-only) | `module`          | Cheap to share within a file                 |
| DataFrame (mutated)   | `function`        | Must be fresh for each test                  |
| Mock / patch          | `function`        | Must not bleed between tests                 |
| Temp directory        | `function`        | Each test needs a clean slate                |

```python
@pytest.fixture(scope="module")
def orders_df(spark):
    rows = generate_list_of_rows("orders", num_rows=50, num_users=10, num_products=10)
    return spark.createDataFrame(rows)
```

**Compose** fixtures rather than building monolithic ones:

```python
@pytest.fixture
def enriched_df(orders_df, users_df):
    """Join orders to users — depends on two narrower fixtures."""
    return orders_df.join(users_df, orders_df["userid"] == users_df["id"], how="inner")
```

---

## Mocking Databricks-only APIs

Databricks-specific functions (e.g. `create_auto_cdc_flow`) are absent from open-source PySpark.  
Patch them onto the module under test *before* the module executes:

```python
from unittest.mock import MagicMock, patch
import pyspark.pipelines as dp_mod

@pytest.fixture
def mock_cdc():
    """Patch Databricks-only create_auto_cdc_flow onto pyspark.pipelines."""
    with patch.object(dp_mod, "create_auto_cdc_flow", MagicMock(), create=True) as m:
        yield m
```

Verify the call arguments explicitly — avoid asserting only call count:

```python
def test_cdc_uses_correct_source(registry, mock_cdc) -> None:
    pipeline.run()
    assert mock_cdc.call_args.kwargs["source"] == "catalog.silver.orders_staging"
```

---

## Loading notebook modules for testing

Databricks notebooks reference `spark` and `dp` globals that aren't importable normally.  
Load them with `importlib.util` and the module executes in the current context where those
globals have already been patched or are irrelevant to the structural tests:

```python
import importlib.util
from pathlib import Path

def _load(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_PIPELINE = _load(Path(__file__).parent.parent / "src/python/notebooks/02.simple_pipeline.py")
```

This allows testing the *structure* (registration, constant values, function signatures) of a notebook
without a full Databricks Runtime.

---

## Marking tests by environment

Declare a `spark` marker for any test that creates or exercises a SparkSession.
This lets CI run pure-Python tests quickly and Spark tests separately:

```toml
# pyproject.toml
[tool.pytest.ini_options]
markers = ["spark: requires a live SparkSession"]
```

Apply at the module level to avoid per-function decoration:

```python
# top of test_transformations.py
pytestmark = pytest.mark.spark
```

Run selectively:

```bash
pytest tests/ -m "not spark"   # fast unit tests only
pytest tests/ -m spark         # integration / Spark tests
```

---

## `autouse` fixtures for cleanup

Use `autouse=True` for side-effect teardown that applies to every test in a module,
such as removing temp files:

```python
@pytest.fixture(autouse=True)
def _clean_tmp():
    """Remove the temp root before and after every test."""
    if os.path.exists(_TMP_ROOT):
        shutil.rmtree(_TMP_ROOT)
    yield
    if os.path.exists(_TMP_ROOT):
        shutil.rmtree(_TMP_ROOT)
```

---

## Summary Checklist

- [ ] Tests that differ only by input data use `@pytest.mark.parametrize` instead of duplicated functions.
- [ ] Module-constant checks are merged into a single parametrized `test_*_constant` function.
- [ ] SparkSession fixture is `scope="session"`; DataFrames consumed as read-only are `scope="module"`.
- [ ] Databricks-only APIs are patched with `patch.object(..., create=True)` in a dedicated fixture.
- [ ] Notebooks are loaded via `importlib.util` for structural testing without runtime globals.
- [ ] The `spark` marker is applied at module level via `pytestmark`; CI runs `not spark` and `spark` separately.
- [ ] `autouse` cleanup fixtures handle temp directories and other side effects.
