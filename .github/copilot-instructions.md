# AI Agent Instructions

These instructions apply to all AI agents (GitHub Copilot, coding agents, and any automated tooling) working in this repository.

---

## ⛔ GUARDRAIL: Run the Full Test Suite Before Creating a PR

> **This is a hard requirement. Do NOT open or submit a pull request without completing all of the steps below.**

Before creating any pull request, you **must** run the full test suite locally:

```bash
# 1. Run all pre-commit hooks against every file
pre-commit run --all-files

# 2. Run pure-Python unit tests (no SparkSession required)
pytest tests/ -m "not spark" -v

# 3. Run PySpark unit tests (SparkSession required)
pytest tests/ -m spark -v
```

**All three commands must pass with zero errors before a PR is opened.**
If any hook or test fails, fix the issue first — then re-run all checks from the top.
Submitting a PR with failing tests or lint errors is not acceptable under any circumstances.

---

## Code Style

### Palantir PySpark Style Guide

All PySpark code must follow the [Palantir PySpark Style Guide](https://github.com/palantir/pyspark-style-guide).
The key rules are captured in the skill file at:

```
.github/copilot/skills/pyspark-style-guide/SKILL.md
```

Highlights:
- Reference columns by string name (`'colA'`) over `df.colA` or `F.col()` unless disambiguation is needed.
- Limit filter/`when` expressions to three sub-expressions; extract complex conditions into named variables.
- Use `select` as a schema contract; prefer `select + alias` over `withColumnRenamed`.
- Use `F.lit(None)` for placeholder columns — never empty strings or sentinel values.
- Avoid UDFs; use native `pyspark.sql.functions` instead.
- Always specify `how=` as a keyword argument in joins; prefer `left` over `right`.
- Always specify an explicit window frame (`rowsBetween` / `rangeBetween`).
- Keep chains to five statements or fewer; group operations by type.
- Keep files under 250 lines and functions under 70 lines.

### PEP 8 Python Style Guide

All Python code must conform to [PEP 8](https://peps.python.org/pep-0008/).
A concise reference skill file is available at:

```
.github/copilot/skills/pep8-style-guide/SKILL.md
```

The project uses **ruff** for automated formatting and linting (configured in `pyproject.toml`).
Running `pre-commit run --all-files` will apply ruff automatically.

### Functional Programming Style Guide

All Python business logic should follow functional programming principles where appropriate.
The skill file at:

```
.github/copilot/skills/functional-programming/SKILL.md
```

covers:
- Writing **pure functions** (no side effects, deterministic output).
- Using **immutable configs** via `NamedTuple` or `@dataclass(frozen=True)`.
- Applying **higher-order functions**, `functools.partial`, and `functools.lru_cache`.
- Building **composable pipelines** with small, named, testable functions.
- Using **generators** for lazy evaluation of large sequences.
- Testing pure functions with `@pytest.mark.parametrize` — no mocking needed.

---

## Consult Databricks Documentation When in Doubt

If you are unsure about Databricks behaviour, APIs, or best practices, always consult the official documentation before proceeding:

- **Azure Databricks**: <https://learn.microsoft.com/en-us/azure/databricks/>
- **AWS Databricks**: <https://docs.databricks.com/aws/en/>

Do not guess or assume Databricks behaviour — look it up.

---

## Summary Checklist for Every PR

- [ ] `pre-commit run --all-files` passes with zero errors
- [ ] `pytest tests/ -m "not spark" -v` passes with zero failures
- [ ] `pytest tests/ -m spark -v` passes with zero failures
- [ ] Code follows the Palantir PySpark Style Guide (`.github/copilot/skills/pyspark-style-guide/SKILL.md`)
- [ ] Code follows PEP 8 (`.github/copilot/skills/pep8-style-guide/SKILL.md`)
- [ ] Business logic functions are pure and tested with `@pytest.mark.parametrize` where applicable (`.github/copilot/skills/functional-programming/SKILL.md`)
- [ ] Any Databricks-specific behaviour was verified against the official docs
