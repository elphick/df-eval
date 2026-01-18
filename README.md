# df-eval

[![CI](https://github.com/elphick/df-eval/actions/workflows/ci.yml/badge.svg)](https://github.com/elphick/df-eval/actions/workflows/ci.yml)
[![Documentation](https://github.com/elphick/df-eval/actions/workflows/docs.yml/badge.svg)](https://github.com/elphick/df-eval/actions/workflows/docs.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A lightweight expression evaluation engine for pandas DataFrames, supporting schema-driven derived columns and external lookups.

## Overview

**df-eval** is a Python library that provides a flexible and efficient way to evaluate expressions on pandas DataFrames. It's designed for scenarios where you need to:

- Apply complex transformations to DataFrames using string expressions
- Define schemas of derived columns that depend on existing columns
- Register custom functions (UDFs) and constants for use in expressions
- Use safe, allow-listed functions (abs, log, exp, sqrt, clip, where, isna, fillna)
- Handle dependencies between derived columns with automatic topological ordering
- Perform lookups from external data sources (files, databases, HTTP APIs)
- Track provenance of derived columns
- Maintain clean, readable code for data transformations

## Features

- **Safe Expression Evaluation**: Allow-listed vectorized functions for secure evaluation
- **UDF and Constant Registry**: Register custom functions and constants
- **Schema-Driven Columns**: Define multiple derived columns with automatic dependency resolution
- **Topological Ordering**: Automatically resolve dependencies between columns
- **Cycle Detection**: Detect and report circular dependencies
- **Dtype Casting**: Specify output types for derived columns
- **Provenance Tracking**: Track the origin and dependencies of derived columns
- **Lookup Functionality**: Resolve values from external sources with caching
- **Type-Safe**: Built with Python 3.11+ type hints
- **Well-Tested**: Comprehensive test suite with 95%+ coverage
- **Well-Documented**: Full documentation with Sphinx
- **Backend Seam**: Designed for future Arrow/Polars support

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

### Basic Expression Evaluation

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
```

### Schema-Driven Derived Columns

```python
# Define a schema with dependent columns
schema = {
    "sum": "a + b",
    "product": "a * b",
    "ratio": "a / b"
}

df_with_derived = engine.apply_schema(df, schema)
print(df_with_derived)
```

### Using Allow-Listed Safe Functions

```python
# Use safe, allow-listed functions
schema = {
    "abs_a": "abs(a)",
    "log_b": "log(b)",
    "sqrt_sum": "sqrt(a + b)",
    "clipped": "clip(a, 0, 2)"
}

result = engine.apply_schema(df, schema)
```

### Register Custom Functions (UDFs)

```python
# Register a custom function
def custom_transform(x):
    return x ** 2 + 10

engine.register_function("transform", custom_transform)

# Use it in expressions
result = engine.evaluate(df, "transform(a)")
```

## Built-in Functions

The library provides several allow-listed safe functions:

- `abs(x)`: Absolute value
- `log(x)`: Natural logarithm (handles negative values safely)
- `exp(x)`: Exponential function (handles overflow safely)
- `sqrt(x)`: Square root (handles negative values safely)
- `clip(x, min, max)`: Clip values to a range
- `where(condition, x, y)`: Conditional selection
- `isna(x)`: Check for NaN/None values
- `fillna(x, value)`: Fill NaN/None with a value
- `safe_divide(a, b)`: Division with NaN for divide-by-zero
- `coalesce(*args)`: Return first non-null value

## Documentation

For comprehensive documentation including advanced usage, API reference, and more examples, visit the [full documentation](https://elphick.github.io/df-eval/).

## Requirements

- Python 3.11 or higher
- pandas >= 2.0.0
- numpy >= 1.26.0

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

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
