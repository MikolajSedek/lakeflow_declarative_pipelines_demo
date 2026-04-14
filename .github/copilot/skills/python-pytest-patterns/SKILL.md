---
name: python-pytest-patterns
description: Write expressive, maintainable pytest test suites using parametrize, fixtures, marks, and modern assertion patterns.
---

When writing or reviewing Python tests with pytest, follow these patterns to maximise readability, coverage, and maintainability.

## `@pytest.mark.parametrize` — eliminate repetitive test functions

Consolidate multiple test functions that exercise the same logic with different inputs into a single parametrized test. This keeps test intent clear while shrinking the test-function count.

```python
# bad — four functions testing the same code path
def test_clamp_within_range():
    assert clamp(5.0, 0.0, 10.0) == 5.0

def test_clamp_below_min():
    assert clamp(-1.0, 0.0, 10.0) == 0.0

def test_clamp_above_max():
    assert clamp(15.0, 0.0, 10.0) == 10.0

def test_clamp_at_boundary():
    assert clamp(0.0, 0.0, 10.0) == 0.0

# good — one function, four cases
@pytest.mark.parametrize(
    ("value", "lo", "hi", "expected"),
    [
        (5.0, 0.0, 10.0, 5.0),
        (-1.0, 0.0, 10.0, 0.0),
        (15.0, 0.0, 10.0, 10.0),
        (0.0, 0.0, 10.0, 0.0),
    ],
)
def test_clamp(value: float, lo: float, hi: float, expected: float) -> None:
    """Should clamp value to [lo, hi] for all boundary and interior cases."""
    assert clamp(value, lo, hi) == expected
```

### When to use parametrize

- Two or more functions share the same assertion logic and differ only in inputs/outputs.
- Testing a pure function across valid values, edge cases, and boundary values.
- Verifying that a group of related constants or configurations hold expected values.

### When NOT to use parametrize

- Each case has meaningfully different setup, teardown, or fixture requirements.
- Cases require fundamentally different assertions (test different *behaviours*, not just different *data*).
- A case needs its own docstring explaining a unique scenario.

---

## Fixtures — share setup without coupling tests

Use fixtures to provide shared, reusable test data and resources. Prefer **function scope** (default) for mutable objects; use **session scope** only for expensive, read-only resources (e.g., a SparkSession).

```python
# conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Shared SparkSession — created once per test session."""
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

@pytest.fixture
def sample_df(spark: SparkSession):
    """Fresh two-column DataFrame for each test."""
    return spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
```

### Fixture guidelines

- Place shared fixtures in `conftest.py`; keep test-file-specific fixtures in the test file.
- Name fixtures after the *thing* they provide, not the *action* that creates it (`user_df`, not `create_user_df`).
- Use `autouse=True` sparingly — only for setup that genuinely applies to *every* test in scope.
- Combine fixtures via composition rather than monolithic "god" fixtures.

---

## `pytest.mark` — organise and filter tests

Declare custom markers in `pyproject.toml` and apply them to control which tests run in different environments.

```toml
# pyproject.toml
[tool.pytest.ini_options]
markers = [
    "spark: tests that require a live SparkSession",
    "integration: tests that hit external systems",
]
```

```python
# apply at module level to mark every test in the file
pytestmark = pytest.mark.spark

# or per-function
@pytest.mark.integration
def test_reads_from_database(db_conn) -> None:
    ...
```

Run subsets:
```bash
pytest -m "not spark"      # skip all Spark tests
pytest -m "spark"          # run only Spark tests
pytest -m "spark and not integration"
```

---

## `pytest.raises` — assert exceptions precisely

Always use `match=` to assert the exception message when the message is meaningful. This prevents false passes from unrelated errors.

```python
# bad — catches any ValueError
def test_negative_input_raises():
    with pytest.raises(ValueError):
        compute(-1)

# good — asserts the specific message
def test_negative_input_raises_value_error():
    with pytest.raises(ValueError, match="must be non-negative"):
        compute(-1)
```

Combine with parametrize to test multiple invalid inputs efficiently:

```python
@pytest.mark.parametrize("invalid", [-1, -10, -100])
def test_negative_inputs_raise(invalid: int) -> None:
    """Should raise ValueError for any negative input."""
    with pytest.raises(ValueError, match="must be non-negative"):
        compute(invalid)
```

---

## Assertion style

- Use plain `assert` — pytest rewrites it to produce rich failure messages.
- Put the expected value **second**: `assert result == expected` (matches `assert actual == expected` reading left-to-right).
- Keep one logical assertion per test function; avoid `and` in assertions.
- Prefer set comparisons for order-independent checks: `assert set(result) == {"a", "b"}`.

```python
# bad — multiple unrelated assertions that break independently
def test_transform():
    result = transform(df)
    assert result.count() == 3
    assert "new_col" in result.columns
    assert result.first()["id"] == 1

# good — split into focused tests
def test_transform_row_count(sample_df):
    assert transform(sample_df).count() == 3

def test_transform_adds_column(sample_df):
    assert "new_col" in transform(sample_df).columns
```

---

## Naming conventions

Follow the `test_<unit>_<scenario>_<expected_outcome>` pattern:

| Unit          | Scenario                  | Expected outcome              |
|---------------|---------------------------|-------------------------------|
| `add`         | `negative_operands`       | `returns_negative_sum`        |
| `parse_date`  | `invalid_format`          | `raises_value_error`          |
| `generate_rows` | `zero_count`            | `returns_empty_list`          |

```python
def test_generate_rows_zero_count_returns_empty_list() -> None:
    assert generate_rows(0) == []
```

Docstrings should complete the sentence *"Should …"*:
```python
def test_parse_date_invalid_format_raises_value_error() -> None:
    """Should raise ValueError when the date string does not match ISO 8601."""
    with pytest.raises(ValueError):
        parse_date("not-a-date")
```

---

## Organise tests with sections

Use comment banners to group related tests within a file. This makes navigation faster and signals intent without splitting into many small files.

```python
# ---------------------------------------------------------------------------
# Tests: happy path
# ---------------------------------------------------------------------------

def test_...

# ---------------------------------------------------------------------------
# Tests: edge cases
# ---------------------------------------------------------------------------

def test_...

# ---------------------------------------------------------------------------
# Tests: error handling
# ---------------------------------------------------------------------------

def test_...
```

---

## Summary Checklist

- [ ] Groups of tests that share the same assertion pattern and differ only by data use `@pytest.mark.parametrize`.
- [ ] Fixtures live in `conftest.py` (shared) or the test file (local); they are composed, not monolithic.
- [ ] Custom marks are declared in `pyproject.toml` and applied at module or function level.
- [ ] `pytest.raises` always includes `match=` when the message is verifiable.
- [ ] Each test function contains one logical assertion.
- [ ] Test names follow `test_<unit>_<scenario>` and docstrings begin with "Should …".
- [ ] Related tests are grouped under section comment banners.
