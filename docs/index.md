# df-eval

A lightweight expression evaluation engine for pandas DataFrames, supporting schema-driven derived columns and external lookups.

## Overview

**df-eval** is a Python library that provides a flexible and efficient way to evaluate expressions on pandas DataFrames. It's designed for scenarios where you need to:

- Apply complex transformations to DataFrames using string expressions
- Define schemas of derived columns that depend on existing columns
- Register custom functions for use in expressions
- Maintain clean, readable code for data transformations

## Features

- **Simple Expression Evaluation**: Evaluate string expressions on DataFrames
- **Schema-Driven Columns**: Define multiple derived columns at once
- **Custom Functions**: Register and use custom functions in expressions
- **Type-Safe**: Built with Python 3.11+ type hints
- **Well-Tested**: Comprehensive test suite with pytest
- **Well-Documented**: Full documentation with Sphinx

## Installation

```bash
pip install df-eval
```

For development:

```bash
git clone https://github.com/elphick/df-eval.git
cd df-eval
uv sync
```

## Quick Start

```python
import pandas as pd
from df_eval import Engine

# Create a DataFrame
df = pd.DataFrame({
    "a": [1, 2, 3],
    "b": [4, 5, 6]
})

# Create an engine
engine = Engine()

# Evaluate an expression
result = engine.evaluate(df, "a + b")
print(result)  # [5, 7, 9]

# Apply a schema of derived columns
schema = {
    "sum": "a + b",
    "product": "a * b",
    "ratio": "a / b"
}
df_with_derived = engine.apply_schema(df, schema)
print(df_with_derived)
```

## Documentation Contents

```{toctree}
:maxdepth: 2
:caption: Contents:

api
```

## API Reference

### Engine

```{eval-rst}
.. autoclass:: df_eval.Engine
   :members:
   :special-members: __init__
```

### Expression

```{eval-rst}
.. autoclass:: df_eval.Expression
   :members:
   :special-members: __init__
```

### Built-in Functions

The library provides several built-in functions:

- `safe_divide(a, b)`: Safely divide two values, returning NaN for division by zero
- `coalesce(*args)`: Return the first non-null value from the arguments
- `clip(value, min_val, max_val)`: Clip values to a specified range

## Requirements

- Python 3.11 or higher
- pandas >= 2.0.0

## Development

### Running Tests

```bash
uv run pytest
```

### Building Documentation

```bash
cd docs
uv run sphinx-build -b html . _build/html
```

### Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Indices and tables

```{eval-rst}
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
```
