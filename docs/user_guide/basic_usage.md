# Basic Usage

This guide covers the fundamental concepts and basic usage patterns of df-eval.

## Creating an Engine

The `Engine` is the main entry point for df-eval. Create one to start evaluating expressions:

```python
from df_eval import Engine

engine = Engine()
```

## Simple Expression Evaluation

Evaluate a single expression on a DataFrame:

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

The `evaluate` method returns a pandas Series with the result.

## Schema-Driven Derived Columns

Define multiple derived columns at once using a schema dictionary:

```python
# Define a schema with multiple derived columns
schema = {
    "sum": "a + b",
    "product": "a * b",
    "ratio": "a / b"
}

# Apply the schema to create new columns
df_with_derived = engine.apply_schema(df, schema)
print(df_with_derived)
#    a  b  sum  product  ratio
# 0  1  4    5        4   0.25
# 1  2  5    7       10   0.40
# 2  3  6    9       18   0.50
```

The `apply_schema` method returns a new DataFrame with the derived columns added.

## Using Built-in Functions

df-eval provides several built-in safe functions that you can use in expressions:

### Mathematical Functions

```python
schema = {
    "abs_value": "abs(a - 5)",
    "sqrt_value": "sqrt(b)",
    "log_value": "log(b)",
    "exp_value": "exp(a)"
}

result = engine.apply_schema(df, schema)
```

### Clipping Values

```python
# Clip values to a range
schema = {
    "clipped": "clip(a, 1, 2)"  # Keep values between 1 and 2
}

result = engine.apply_schema(df, schema)
print(result["clipped"])  # [1, 2, 2]
```

### Conditional Operations

```python
# Use where for conditional logic
schema = {
    "category": "where(a > 2, 'high', 'low')"
}

result = engine.apply_schema(df, schema)
print(result["category"])  # ['low', 'low', 'high']
```

### Handling Missing Values

```python
df_with_nulls = pd.DataFrame({
    "a": [1, None, 3],
    "b": [4, 5, None]
})

schema = {
    "has_null": "isna(a)",
    "filled": "fillna(b, 0)"
}

result = engine.apply_schema(df_with_nulls, schema)
```

### Safe Division

```python
# Avoid division by zero errors
schema = {
    "safe_ratio": "safe_divide(a, b)"
}

# Returns NaN for division by zero instead of raising an error
result = engine.apply_schema(df, schema)
```

### Coalesce

```python
# Return first non-null value
df_multi = pd.DataFrame({
    "a": [1, None, None],
    "b": [None, 2, None],
    "c": [None, None, 3]
})

schema = {
    "first_valid": "coalesce(a, b, c)"
}

result = engine.apply_schema(df_multi, schema)
print(result["first_valid"])  # [1, 2, 3]
```

## Batch Evaluation

Evaluate multiple independent expressions at once:

```python
expressions = {
    "sum": "a + b",
    "product": "a * b",
    "avg": "(a + b) / 2"
}

# Evaluate all expressions
results = engine.evaluate_many(df, expressions)

# results is a dictionary mapping names to Series
for name, series in results.items():
    print(f"{name}: {series.tolist()}")
```

## Specifying Data Types

Control the output type of derived columns:

```python
from df_eval import ColumnSpec

schema = {
    "float_sum": ColumnSpec("a + b", dtype="float64"),
    "int_product": ColumnSpec("a * b", dtype="int32")
}

result = engine.apply_schema(df, schema)
print(result.dtypes)
```

## Error Handling

df-eval validates expressions and provides clear error messages:

```python
try:
    # This will fail - column 'z' doesn't exist
    result = engine.evaluate(df, "z + 1")
except Exception as e:
    print(f"Error: {e}")
```

## Next Steps

- Learn about [Advanced Usage](advanced_usage.md) for dependency management and custom functions
- Explore [Lookups](lookups.md) for integrating external data sources
