---
name: pytest
description: Write, structure, and maintain high-quality pytest test suites following official pytest documentation best practices.
---

When writing or reviewing pytest tests, follow the rules and patterns below, derived from the
[pytest documentation](https://docs.pytest.org/en/stable/).

---

## Anatomy of a Test: Arrange – Act – Assert – Cleanup

Every test should be decomposed into four clear phases:

1. **Arrange** – set up the preconditions (data, objects, services). Delegate this to fixtures.
2. **Act** – the single state-changing call that exercises the behaviour under test.
3. **Assert** – check the resulting state; use plain `assert` statements.
4. **Cleanup** – tear down any resources (use `yield` fixtures so cleanup is automatic).

```python
def test_email_received(sending_user, receiving_user):
    # Act
    sending_user.send_email(Email("Hi", "Body"), receiving_user)
    # Assert
    assert Email("Hi", "Body") in receiving_user.inbox
```

---

## Fixtures

### Define fixtures with `@pytest.fixture`

```python
import pytest

@pytest.fixture
def user():
    return User(name="Alice")
```

### Use `yield` for teardown (preferred over `addfinalizer`)

```python
@pytest.fixture
def db_connection():
    conn = create_connection()
    yield conn          # test runs here
    conn.close()        # teardown always executes
```

### Scope fixtures to the right lifetime

| Scope | Destroyed after |
|-------|----------------|
| `function` (default) | each test function |
| `class` | last test in the class |
| `module` | last test in the module |
| `package` | last test in the package |
| `session` | end of the entire test session |

Use `scope="session"` or `scope="module"` for expensive resources (SparkSession, DB connections):

```python
@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[1]").getOrCreate()
    yield session
    session.stop()
```

### Share fixtures via `conftest.py`

Place fixtures used across multiple test files in `conftest.py`. Pytest discovers them automatically — no import needed.

```
tests/
    conftest.py       ← shared fixtures live here
    test_users.py
    test_orders.py
```

### Use `autouse=True` for setup that applies to every test in scope

```python
@pytest.fixture(autouse=True)
def clean_tmp_dir(tmp_path):
    yield
    shutil.rmtree(tmp_path, ignore_errors=True)
```

### Prefer `tmp_path` over manual temp directories

```python
def test_writes_file(tmp_path):
    output = tmp_path / "result.csv"
    write_data(output)
    assert output.exists()
```

### Use `monkeypatch` for environment and attribute patching

```python
def test_reads_env_var(monkeypatch):
    monkeypatch.setenv("API_KEY", "test-key")
    assert get_api_key() == "test-key"
```

### Use `caplog` for log assertions

```python
def test_logs_warning(caplog):
    import logging
    with caplog.at_level(logging.WARNING):
        process_data([])
    assert "no rows" in caplog.text
```

### Use `capsys` / `capfd` for stdout/stderr assertions

```python
def test_prints_header(capsys):
    print_report_header()
    captured = capsys.readouterr()
    assert "Report" in captured.out
```

---

## Parametrize

### Use `@pytest.mark.parametrize` to eliminate duplicate test functions

When multiple test functions differ only in their inputs and expected outputs, collapse them into one parametrized function:

```python
# bad – three functions testing the same logic
def test_add_two_and_three():
    assert add(2, 3) == 5

def test_add_zero_and_five():
    assert add(0, 5) == 5

def test_add_negative():
    assert add(-1, 1) == 0


# good – one parametrized function
@pytest.mark.parametrize(("a", "b", "expected"), [
    (2, 3, 5),
    (0, 5, 5),
    (-1, 1, 0),
])
def test_add(a, b, expected):
    assert add(a, b) == expected
```

### Use `pytest.param` for readable IDs and per-case marks

```python
@pytest.mark.parametrize(("value", "expected"), [
    pytest.param("hello", True, id="valid-string"),
    pytest.param("", False, id="empty-string"),
    pytest.param(None, False, id="none-value", marks=pytest.mark.xfail),
])
def test_is_valid(value, expected):
    assert is_valid(value) == expected
```

### Stack decorators for combinatorial coverage

```python
@pytest.mark.parametrize("fmt", ["csv", "parquet", "json"])
@pytest.mark.parametrize("compression", [None, "gzip", "snappy"])
def test_write_format(fmt, compression):
    ...  # runs 9 combinations
```

### Use `indirect=True` to pass parameters through a fixture

```python
@pytest.fixture
def db(request):
    return connect(request.param)

@pytest.mark.parametrize("db", ["sqlite", "postgres"], indirect=True)
def test_insert(db):
    db.execute("INSERT INTO t VALUES (1)")
```

---

## Markers

### Register custom markers in `pyproject.toml` to avoid warnings

```toml
[tool.pytest.ini_options]
markers = [
    "spark: tests requiring a live SparkSession",
    "integration: slow end-to-end tests",
]
```

### Apply markers at module level with `pytestmark`

```python
pytestmark = pytest.mark.spark
```

### Use built-in markers for known conditions

```python
@pytest.mark.skip(reason="upstream API not available in CI")
def test_external_call(): ...

@pytest.mark.skipif(sys.platform == "win32", reason="POSIX paths only")
def test_posix_paths(): ...

@pytest.mark.xfail(reason="known bug #123", strict=True)
def test_known_failure(): ...
```

---

## Assertions

### Use plain `assert` — pytest rewrites it for rich failure messages

```python
assert result == expected           # pytest shows both values on failure
assert "error" in message
assert df.count() > 0
```

### Use `pytest.approx` for floating-point comparisons

```python
assert 0.1 + 0.2 == pytest.approx(0.3)
assert computed_revenue == pytest.approx(expected_revenue, rel=1e-3)
```

### Use `pytest.raises` as a context manager for exception testing

```python
with pytest.raises(ValueError, match="must be positive"):
    process(value=-1)
```

Always include a `match=` pattern — it guards against catching the wrong `ValueError`.

### Inspect `ExceptionInfo` when you need the exception object

```python
with pytest.raises(RuntimeError) as exc_info:
    risky_operation()
assert "timeout" in str(exc_info.value)
assert exc_info.type is RuntimeError
```

---

## Test Discovery and Layout

### Preferred project layout (`src` layout)

```
pyproject.toml
src/
    mypackage/
        module.py
tests/
    conftest.py
    test_module.py
```

Configure `pythonpath` in `pyproject.toml` when using `src` layout:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src"]
```

### Naming conventions (pytest discovers by default)

- Test files: `test_*.py` or `*_test.py`
- Test functions/methods: `test_*`
- Test classes: `Test*` (no `__init__` method)

---

## Configuration (`pyproject.toml`)

```toml
[tool.pytest.ini_options]
testpaths     = ["tests"]
pythonpath    = ["src"]
addopts       = ["-ra", "--strict-markers", "--strict-config"]
markers       = [
    "spark: tests requiring a live SparkSession",
    "slow: long-running integration tests",
]
```

Key options:
- `--strict-markers`: turns undeclared marker usage into an error.
- `--strict-config`: turns config warnings into errors.
- `-ra`: show short summary of all non-passing tests at the end.

---

## Coverage with `pytest-cov`

Install `pytest-cov`:

```bash
pip install pytest-cov
```

Run with an annotated coverage report:

```bash
pytest --cov --cov-report=annotate:cov_annotate
```

For a specific module:

```bash
pytest --cov=mypackage --cov-report=annotate:cov_annotate tests/
```

Inspect `cov_annotate/`: lines starting with `!` are **not covered**.
Add tests until all `!` lines are covered.

Enforce a minimum threshold in CI:

```bash
pytest --cov=mypackage --cov-fail-under=90
```

---

## Skipping and Conditional Execution

### Skip dynamically with `pytest.skip()`

```python
def test_gpu_feature():
    if not torch.cuda.is_available():
        pytest.skip("no GPU")
    ...
```

### Use `pytest.importorskip` for optional dependencies

```python
np = pytest.importorskip("numpy")

def test_array_sum():
    assert np.sum([1, 2, 3]) == 6
```

---

## Mocking

Prefer `unittest.mock` (stdlib) or `pytest-mock` (`mocker` fixture).

```python
from unittest.mock import MagicMock, patch

def test_calls_api(monkeypatch):
    mock_client = MagicMock()
    monkeypatch.setattr("mymodule.api_client", mock_client)
    call_service()
    mock_client.post.assert_called_once()
```

Use `patch.object` for patching attributes on live modules:

```python
with patch.object(module, "expensive_func", return_value=42):
    assert compute() == 42
```

---

## Pure Functions and Parametrize (Best Combination)

Pure functions (no side effects, deterministic output) are the easiest to test.
Always test them with `@pytest.mark.parametrize`:

```python
@pytest.mark.parametrize(("row_type", "num_rows", "expected_len"), [
    ("users",    5,  5),
    ("products", 0,  0),
    ("orders",   10, 10),
])
def test_generate_rows_count(row_type, num_rows, expected_len):
    kwargs = {"num_users": 20, "num_products": 20} if row_type == "orders" else {}
    rows = generate_list_of_rows(row_type, num_rows=num_rows, **kwargs)
    assert len(rows) == expected_len
```

---

## Anti-Patterns to Avoid

| Anti-pattern | What to do instead |
|---|---|
| Multiple test functions that differ only in data | Use `@pytest.mark.parametrize` |
| `setup`/`teardown` methods (xUnit style) | Use `yield` fixtures |
| `import` inside test bodies | Import at module level or in `conftest.py` |
| `print()` for debugging | Use `capfd`/`capsys`, or `-s` flag |
| Hardcoded absolute paths | Use `tmp_path` fixture or `pathlib.Path` |
| Bare `pytest.raises(Exception)` | Always include `match=` pattern |
| Session-level fixtures with `scope="function"` for heavy setup | Use `scope="session"` or `scope="module"` |
| Asserting `True` / `False` directly on method calls | Assert the actual value: `assert compute() == expected` |
| Tests that depend on execution order | Make each test independent; use fixtures for shared state |
| Catching broad exceptions in `pytest.raises` | Specify the exact exception type and a `match=` regex |

---

## Useful CLI Flags

| Flag | Purpose |
|------|---------|
| `-v` | verbose: show each test name |
| `-ra` | show summary of all non-passing results |
| `-k "expr"` | run tests whose name matches expression |
| `-m "marker"` | run tests with a specific marker |
| `--co` / `--collect-only` | list collected tests without running |
| `--fixtures` | list available fixtures |
| `-x` / `--exitfirst` | stop after first failure |
| `--lf` / `--last-failed` | re-run only previously failed tests |
| `-s` | disable output capture (see `print` output) |
| `--tb=short` | shorter traceback format |
| `--strict-markers` | error on undeclared markers |
