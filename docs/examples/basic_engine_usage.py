"""Basic Engine Usage
=====================

This example walks through core :class:`df_eval.Engine` patterns on a
small in-memory DataFrame.

It covers:

- Creating an :class:`~df_eval.engine.Engine`
- Evaluating a single expression with :meth:`Engine.evaluate`
- Defining multiple derived columns with :meth:`Engine.apply_schema`
- Using a few built-in safe functions
- Evaluating multiple independent expressions with
  :meth:`Engine.evaluate_many`
"""

import pandas as pd

from df_eval import Engine


# %%
# Build Input Data
# ----------------

df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
df


# %%
# Create Engine, Single Expression
# --------------------------------

engine = Engine()

single_result = engine.evaluate(df, "a + b")
single_result


# %%
# Schema-Driven Derived Columns
# -----------------------------

schema = {
    "sum": "a + b",
    "product": "a * b",
    "ratio": "a / b",
    "safe_ratio": "safe_divide(a, b)",
}

df_with_derived = engine.apply_schema(df, schema)
df_with_derived


# %%
# Controlling Output Types with ``dtypes``
# ----------------------------------------

typed_schema = {
    "float_sum": "a + b",
    "int_product": "a * b",
}

typed_result = engine.apply_schema(
    df,
    typed_schema,
    dtypes={"float_sum": "float64", "int_product": "int32"},
)
typed_result.dtypes


# %%
# Evaluating Multiple Independent Expressions
# -------------------------------------------

expressions = {
    "sum": "a + b",
    "product": "a * b",
    "avg": "(a + b) / 2",
}

many_results = engine.evaluate_many(df, expressions)

{
    name: series.tolist() for name, series in many_results.items()
}


