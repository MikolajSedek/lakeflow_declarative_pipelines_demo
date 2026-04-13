---
name: pep8-style-guide
description: Write idiomatic, readable Python code following PEP 8 – the official Python style guide.
---

When writing or reviewing Python code, follow these rules derived from [PEP 8 – Style Guide for Python Code](https://peps.python.org/pep-0008/).

## Indentation

Use **4 spaces** per indentation level. Never use tabs.

Continuation lines should align with the opening delimiter, or use a hanging indent with a distinguishing blank line before the first argument:

```python
# aligned with opening delimiter
foo = long_function_name(var_one, var_two,
                         var_three, var_four)

# hanging indent – 4 extra spaces to distinguish from the body
def long_function_name(
        var_one, var_two,
        var_three, var_four):
    print(var_one)
```

Closing brackets may line up under the last element or under the first character of the line that starts the construct:

```python
# good
result = some_function_that_takes_arguments(
    'a', 'b', 'c',
    'd', 'e', 'f',
)
```

## Maximum Line Length

Limit all lines to **79 characters**. Docstrings and comments: **72 characters**.
Wrap long lines using Python's implicit line continuation inside parentheses, brackets, or braces. Never use `\` for line continuation.

## Blank Lines

- Surround **top-level** function and class definitions with **two** blank lines.
- Method definitions inside a class are surrounded by **one** blank line.
- Use blank lines sparingly inside functions to separate logical sections.

## Imports

- Imports should be on **separate lines**.
- Group imports in this order, separated by a blank line:
  1. Standard library imports
  2. Related third-party imports
  3. Local application/library-specific imports
- Use absolute imports; avoid wildcard imports (`from module import *`).

```python
# bad
import sys, os

# good
import os
import sys

from typing import List

from pyspark.sql import SparkSession
```

## Whitespace in Expressions

Avoid extraneous whitespace:

```python
# bad
spam( ham[ 1 ], { eggs : 2 } )

# good
spam(ham[1], {eggs: 2})

# bad
x = x *2- 1

# good
x = x * 2 - 1
```

Always surround these binary operators with a **single space** on each side: `=`, `+=`, `-=`, comparisons, Booleans.
When mixing operators with different priorities, add spaces around the **lowest-priority** operators only:

```python
# good
hypot2 = x*x + y*y
c = (a+b) * (a-b)
```

## Trailing Commas

Use a trailing comma on the last element of a tuple, list, function argument list, or import when each element is on its own line:

```python
# good
FILES = (
    'setup.cfg',
    'tox.ini',
)
```

## Comments

- Keep comments up to date with the code — wrong comments are worse than no comments.
- Write comments as **complete sentences** starting with a capital letter.
- Block comments: each line starts with `# ` and a single space.
- Inline comments: separate from the statement with **two spaces**, use sparingly.
- Use `"""Docstrings"""` for all public modules, functions, classes, and methods.

```python
def fetch_data(url: str) -> dict:
    """Fetch JSON data from the given URL.

    Args:
        url: The endpoint to query.

    Returns:
        Parsed JSON response as a dictionary.
    """
```

## Naming Conventions

| Construct | Convention | Example |
|---|---|---|
| Packages & modules | `lowercase` or `lower_with_underscores` | `my_module` |
| Classes | `CapWords` (PascalCase) | `MyClass` |
| Exceptions | `CapWords` ending with `Error` | `ValueError` |
| Functions & variables | `snake_case` | `my_function` |
| Constants | `ALL_CAPS_WITH_UNDERSCORES` | `MAX_RETRIES` |
| Private names | leading underscore `_name` | `_helper` |
| "Magic" names | double underscores `__name__` | `__init__` |

Avoid single-character names except for counters (`i`, `j`, `k`) or well-known mathematical variables.
Never use `l` (lowercase L), `O` (uppercase O), or `I` (uppercase I) as single-character variable names.

## Type Annotations

Use type annotations for all public function signatures (PEP 484):

```python
# good
def greet(name: str) -> str:
    return f"Hello, {name}"
```

## String Quotes

Be consistent — pick single or double quotes and stick with it throughout a file.
Use the other style to avoid backslashes inside a string:

```python
# good – avoids escape
message = "Don't do this"
```

## Programming Recommendations

- Use `is` / `is not` to compare with `None`, never `==` or `!=`.
- Use `if seq:` / `if not seq:` to test empty sequences, not `if len(seq) == 0`.
- Use `isinstance()` instead of `type()` for type comparisons.
- Derive exceptions from `Exception`, not `BaseException`.
- Use `with` statements to ensure resources are properly released.
- Return values should be consistent: either always return an expression, or always return `None` implicitly.

```python
# bad
def foo(x):
    if x >= 0:
        return math.sqrt(x)

# good
def foo(x):
    if x >= 0:
        return math.sqrt(x)
    return None
```

## Automated Enforcement

This project uses **ruff** (configured in `pyproject.toml`) to enforce PEP 8 automatically.
Running `pre-commit run --all-files` will apply ruff formatting and flag any remaining style issues.
Always run pre-commit before opening a PR.
