---
name: functional-programming
description: Write clean, testable, and maintainable Python code using functional programming principles — pure functions, immutability, higher-order functions, and composable pipelines.
---

When writing or reviewing Python code with a functional style, follow these principles drawn from the [Python functional programming documentation](https://docs.python.org/3/howto/functional.html) and community best practices.

## Core Principles

Functional programming in Python rests on four pillars:

1. **Pure functions** — output depends solely on inputs; no side effects.
2. **Immutability** — prefer data structures that cannot be changed after creation.
3. **First-class / higher-order functions** — pass and return functions like any other value.
4. **Declarative style** — describe *what* to compute, not *how* to compute it step by step.

Python is a multi-paradigm language: apply FP principles where they improve clarity and testability, without fighting the language.

---

## Pure Functions

A **pure function** always returns the same output for the same input and produces no side effects (no mutations of external state, no I/O, no global variable access).

```python
# bad — reads and mutates external state
total = 0

def add_to_total(x):
    global total
    total += x

# good — pure: output depends only on inputs
def add(a: int, b: int) -> int:
    return a + b
```

**Rules for pure functions:**
- Accept all inputs as parameters; never read from globals or closures that change.
- Return a new value; never mutate an argument in-place.
- Keep I/O (logging, DB writes, HTTP calls) at the boundary of your system, not inside business-logic functions.
- Use `functools.lru_cache` to memoize expensive pure functions safely, because purity guarantees the cache is always correct.

```python
from functools import lru_cache

@lru_cache(maxsize=128)
def fibonacci(n: int) -> int:
    if n < 2:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)
```

---

## Immutability

Prefer immutable data to reduce the risk of accidental mutation bugs, especially in concurrent code.

### Built-in immutable types

| Mutable       | Prefer immutable alternative          |
|---------------|---------------------------------------|
| `list`        | `tuple`                               |
| `dict`        | `types.MappingProxyType` / `NamedTuple` |
| `set`         | `frozenset`                           |

```python
# bad — list can be mutated anywhere
ALLOWED_STATUSES = ['pending', 'active', 'closed']

# good — tuple is immutable
ALLOWED_STATUSES = ('pending', 'active', 'closed')
```

### `NamedTuple` for immutable configs and value objects

Use `typing.NamedTuple` (class syntax) for structured, immutable data. Prefer it over plain `dict` for configs and domain value objects.

```python
from typing import NamedTuple

class PipelineConfig(NamedTuple):
    source_table: str
    target_table: str
    batch_size: int = 1000
    dry_run: bool = False

# Usage — positional or keyword construction
cfg = PipelineConfig(
    source_table='bronze.events',
    target_table='silver.events_clean',
)

# Accessing fields is explicit and IDE-friendly
print(cfg.batch_size)   # 1000
print(cfg._asdict())    # {'source_table': ..., ...}
```

**Why `NamedTuple` over a plain `dataclass` for config objects:**
- Immutable by default (no `__setattr__`).
- Iterable and unpackable like a regular tuple.
- Lightweight — no extra metaclass overhead.
- Works as a dictionary key (hashable).

### Frozen `dataclass` for richer immutable objects

When you need methods, inheritance, or default factories, use `@dataclass(frozen=True)`:

```python
from dataclasses import dataclass, field
from typing import Tuple

@dataclass(frozen=True)
class TransformConfig:
    input_columns: Tuple[str, ...] = field(default_factory=tuple)
    output_column: str = 'result'
    drop_nulls: bool = True

cfg = TransformConfig(input_columns=('a', 'b', 'c'))
# cfg.output_column = 'x'  # raises FrozenInstanceError
```

**Key differences:**

| Feature               | `NamedTuple`              | `frozen=True` dataclass        |
|-----------------------|---------------------------|--------------------------------|
| Immutable             | ✅                         | ✅                              |
| Hashable              | ✅ (if all fields hashable)| ✅ (if all fields hashable)     |
| Inheritance           | limited                   | ✅                              |
| Default factories     | ❌                         | ✅ (`field(default_factory=…)`) |
| Positional unpacking  | ✅                         | ❌                              |

Use `NamedTuple` for simple, flat configs. Use `frozen=True` dataclasses for configs with mutable-default fields or when you need inheritance.

---

## First-Class and Higher-Order Functions

Functions in Python are first-class citizens: assign them to variables, store them in collections, pass them as arguments, and return them from functions.

```python
from typing import Callable

def apply(fn: Callable[[int], int], value: int) -> int:
    return fn(value)

def double(x: int) -> int:
    return x * 2

result = apply(double, 5)  # 10
```

### `lambda` — anonymous functions

Use `lambda` only for simple, single-expression callbacks. For anything more complex, define a named function.

```python
# good — trivial key function
names = ['Alice', 'Bob', 'Charlie']
names.sort(key=lambda s: len(s))

# bad — complex logic crammed into a lambda; define a named function instead
transform = lambda x: x * 2 if x > 0 else abs(x) + 1  # noqa: E731
```

### `map`, `filter`, `reduce`

Prefer **list/generator comprehensions** over `map` and `filter` for readability. Reserve `map`/`filter` for cases where the transformation function is already a named callable.

```python
from functools import reduce
from typing import List

numbers = [1, 2, 3, 4, 5]

# preferred — comprehensions are more Pythonic
squares     = [x ** 2 for x in numbers]
even_nums   = [x for x in numbers if x % 2 == 0]

# acceptable when using an existing named function
import math
roots = list(map(math.sqrt, numbers))

# reduce — use explicitly when folding a sequence to a single value
product = reduce(lambda a, b: a * b, numbers)  # 120
```

---

## `functools` Toolkit

| Utility                 | Purpose                                                      |
|-------------------------|--------------------------------------------------------------|
| `functools.partial`     | Fix some arguments of a function; create specialised callables |
| `functools.lru_cache`   | Memoize pure functions; avoid repeated expensive computation  |
| `functools.reduce`      | Fold a sequence into a single value                          |
| `functools.wraps`       | Preserve wrapped function metadata inside decorators         |
| `operator` module       | Named versions of operators (`operator.add`, `operator.itemgetter`) |

```python
from functools import partial

def power(base: int, exp: int) -> int:
    return base ** exp

square = partial(power, exp=2)
cube   = partial(power, exp=3)

print(square(4))  # 16
print(cube(3))    # 27
```

---

## Function Composition and Pipelines

Break data transformations into small, composable pure functions, then chain them into a pipeline.

```python
from typing import Callable, TypeVar

T = TypeVar('T')


def compose(*fns: Callable) -> Callable:
    """Right-to-left function composition: compose(f, g)(x) == f(g(x))."""
    from functools import reduce
    return reduce(lambda f, g: lambda x: f(g(x)), fns)


def strip(s: str) -> str:
    return s.strip()


def to_lower(s: str) -> str:
    return s.lower()


def remove_punctuation(s: str) -> str:
    return ''.join(c for c in s if c.isalnum() or c.isspace())


normalize = compose(to_lower, strip, remove_punctuation)
print(normalize("  Hello, World!  "))  # "hello world"
```

For data pipelines with many steps, prefer explicit intermediate variables over deeply nested calls:

```python
# bad — hard to debug, hard to read
result = f4(f3(f2(f1(raw_data))))

# good — each step is named and inspectable
step1 = f1(raw_data)
step2 = f2(step1)
step3 = f3(step2)
result = f4(step3)
```

---

## Generators and Lazy Evaluation

Use **generators** to process large or potentially infinite sequences without loading everything into memory.

```python
from typing import Generator, Iterable

def even_numbers(limit: int) -> Generator[int, None, None]:
    """Yield even numbers up to limit (lazy)."""
    for n in range(limit):
        if n % 2 == 0:
            yield n


def transform(items: Iterable[int]) -> Iterable[int]:
    """Chain generator steps — nothing runs until consumed."""
    return (x ** 2 for x in items if x > 0)
```

**Rules:**
- Prefer generator expressions `(… for …)` over list comprehensions when the result is consumed once or passed to another iterator.
- Use `yield from` to delegate to sub-generators, avoiding manual loops.
- Never consume a generator more than once — use `itertools.tee` or materialise with `list()` if multiple passes are needed.

---

## Recursion

Python does **not** optimize tail calls. Keep recursion for naturally hierarchical problems (trees, nested structures) where the depth is bounded and shallow (well below the default 1 000-frame limit).

```python
from typing import List

def flatten(nested: list) -> List:
    """Recursively flatten a nested list."""
    result = []
    for item in nested:
        if isinstance(item, list):
            result.extend(flatten(item))
        else:
            result.append(item)
    return result
```

For deep or large-N recursion, convert to an iterative solution or use `sys.setrecursionlimit` with caution.

---

## Testing Pure Functions

Pure functions are trivially easy to test: no mocking, no setup, no teardown.

```python
# functions under test
def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def normalize_text(text: str) -> str:
    return text.strip().lower()


# tests — parametrised for completeness
import pytest

@pytest.mark.parametrize('value, lo, hi, expected', [
    (5.0, 0.0, 10.0, 5.0),
    (-1.0, 0.0, 10.0, 0.0),
    (15.0, 0.0, 10.0, 10.0),
    (0.0, 0.0, 10.0, 0.0),
])
def test_clamp(value, lo, hi, expected):
    assert clamp(value, lo, hi) == expected


@pytest.mark.parametrize('raw, expected', [
    ('  Hello  ', 'hello'),
    ('WORLD\n', 'world'),
    ('', ''),
])
def test_normalize_text(raw, expected):
    assert normalize_text(raw) == expected
```

**Testing guidelines for FP code:**

- Use `@pytest.mark.parametrize` to cover edge cases without duplicating test boilerplate.
- Test the output value only — no assertions on internal state because there is none.
- Property-based testing with `hypothesis` is a natural fit: pure functions satisfy mathematical properties that can be expressed as universal rules.
- When a function is impure (I/O, DB), extract the pure core and test it independently.

---

## Immutable Configs in Practice

Centralise configuration in immutable objects at the top of a module or passed as parameters — never scatter raw strings and magic numbers through business logic.

```python
from typing import NamedTuple

class IngestionConfig(NamedTuple):
    raw_path: str
    checkpoint_path: str
    max_files_per_trigger: int = 100
    watermark_delay: str = '10 minutes'


def ingest(cfg: IngestionConfig) -> None:
    """Accept config as an immutable value object."""
    # ... use cfg.raw_path, cfg.max_files_per_trigger, etc.
    pass
```

**Rules:**
- Pass config as a single typed object, not as a bag of keyword arguments.
- Derive computed values from the config *outside* the function if they are constants:

```python
# bad — magic literals inside business logic
def process(df, threshold=0.95, output='silver.clean'):
    ...

# good — config object carries all parameters
class ProcessConfig(NamedTuple):
    threshold: float
    output_table: str

cfg = ProcessConfig(threshold=0.95, output_table='silver.clean')
process(df, cfg)
```

---

## What to Avoid

| Anti-pattern                                | Why it is harmful                                      | Remedy                                      |
|---------------------------------------------|--------------------------------------------------------|---------------------------------------------|
| Mutating function arguments                 | Creates hidden coupling between caller and callee      | Return a new object                         |
| Accessing global / module-level mutable state | Makes functions non-deterministic; hard to test       | Pass state as a parameter                   |
| Long `lambda` with complex logic            | Unreadable; impossible to test or document             | Define a named function                     |
| Deeply nested `map(filter(map(...)))`       | Illegible; hard to debug                               | Use comprehensions or named pipeline steps  |
| Recursion for flat loops                    | Hits Python's recursion limit; slower than iteration   | Use `for` / `while` instead                |
| Bare mutable `dict` as config               | No type safety, no IDE support, easy to misspell keys  | Use `NamedTuple` or `frozen=True` dataclass |
| Side effects inside comprehensions          | Confusing; comprehensions should be expression-only    | Move side effects to an explicit `for` loop |

---

## Summary Checklist

- [ ] Every business-logic function is pure: same input → same output, no side effects.
- [ ] Configuration and value objects use `NamedTuple` or `@dataclass(frozen=True)`.
- [ ] Mutable default arguments (`def f(items=[])`) are absent.
- [ ] `lambda` is used only for simple single-expression callbacks.
- [ ] Large collections processed lazily with generators where appropriate.
- [ ] Pure functions are tested with `@pytest.mark.parametrize`; no mocking required.
- [ ] I/O and side effects are pushed to the outermost layer of the call stack.
- [ ] `functools.lru_cache` is applied to expensive, deterministic functions.
