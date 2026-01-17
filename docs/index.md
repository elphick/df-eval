# df-eval

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

### Register Constants

```python
# Register constants
engine.register_constant("PI", 3.14159)
engine.register_constant("THRESHOLD", 100)

# Use them in expressions
result = engine.evaluate(df, "a * PI")
```

### Complex Dependencies with Topological Sorting

```python
# Define columns with dependencies
# The engine automatically determines the correct evaluation order
schema = {
    "b": "a * 2",       # Depends on a
    "c": "b + 10",      # Depends on b
    "d": "c * a"        # Depends on both c and a
}

result = engine.apply_schema(df, schema)
# Columns are evaluated in the correct order: b, c, d
```

### Cycle Detection

```python
# The engine detects circular dependencies
schema = {
    "x": "y + 1",  # Depends on y
    "y": "x + 1"   # Depends on x - creates a cycle!
}

try:
    result = engine.apply_schema(df, schema)
except CycleDetectedError as e:
    print(f"Cycle detected: {e}")
```

### Provenance Tracking

```python
# Enable provenance tracking
engine.enable_provenance(True)

schema = {"derived": "a + b * 2"}
result = engine.apply_schema(df, schema)

# Access provenance information
print(result.attrs['df_eval_provenance'])
# {'derived': {'expression': 'a + b * 2', 'dependencies': ['a', 'b']}}
```

### Lookups with Resolvers

```python
from df_eval import lookup, DictResolver, CachedResolver

# Create a dictionary resolver
mapping = {"apple": 1.50, "banana": 0.75, "orange": 1.25}
resolver = DictResolver(mapping, default=0.0)

# Lookup values
products = pd.Series(["apple", "banana", "cherry"])
prices = lookup(products, resolver, on_missing="null")
print(prices)  # [1.50, 0.75, None]

# Use with caching for expensive lookups
cached_resolver = CachedResolver(resolver, ttl_seconds=300)
```

### File-Based Lookups

```python
from df_eval import FileResolver

# Lookup from CSV file
resolver = FileResolver("prices.csv", key_column="product", value_column="price")
prices = lookup(products, resolver)
```

### Batch Evaluation

```python
# Evaluate multiple expressions at once
expressions = {
    "sum": "a + b",
    "product": "a * b",
    "avg": "(a + b) / 2"
}

result = engine.evaluate_many(df, expressions)
```

## Documentation Contents

```{toctree}
:maxdepth: 2
:caption: Contents:

api
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
