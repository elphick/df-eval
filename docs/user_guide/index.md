# User Guide

This guide provides detailed information on how to use df-eval for various data transformation tasks.

## Getting Started

If you haven't installed df-eval yet, see the Installation section in the main documentation.

## Guide Contents

```{toctree}
:maxdepth: 2

basic_usage
advanced_usage
lookups
```

## What You'll Learn

- **[Basic Usage](basic_usage.md)**: Learn the fundamentals of expression evaluation and schema-driven transformations
- **[Advanced Usage](advanced_usage.md)**: Explore dependency management, provenance tracking, and custom functions
- **[Lookups](lookups.md)**: Master external data lookups and resolver patterns

## Quick Reference

### Creating an Engine

```python
from df_eval import Engine

engine = Engine()
```

### Basic Evaluation

```python
result = engine.evaluate(df, "a + b")
```

### Schema Application

```python
schema = {"derived_col": "a * 2"}
result = engine.apply_schema(df, schema)
```

## Next Steps

Start with [Basic Usage](basic_usage.md) to learn the fundamentals, then progress through the guide at your own pace.
