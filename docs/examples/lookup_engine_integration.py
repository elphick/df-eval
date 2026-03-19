"""
Engine Lookups with Resolvers
=============================

This example shows how to wire df-eval's :class:`Engine` together with
lookup resolvers so you can keep external reference data (like prices or
categories) outside your core DataFrames and use them alongside
expression-driven columns.

Pipeline:

- Build a small input DataFrame
- Register lookup resolvers on the Engine
- Use the ``lookup`` helper in schema expressions
- Inspect the resulting derived columns
"""

import pandas as pd

from df_eval import Engine, DictResolver, lookup


# %%
# Build Input Data
# ----------------
# A simple sales table with product names and quantities.

sales_df = pd.DataFrame(
    {
        "product": ["apple", "banana", "orange"],
        "quantity": [10, 20, 15],
    }
)

sales_df


# %%
# Register Resolvers on the Engine
# --------------------------------
#
# Create in-memory resolvers for product prices and product categories, then
# register them on an :class:`Engine` instance so they can be used from
# expressions via the :func:`lookup` helper.

price_resolver = DictResolver(
    {
        "apple": 1.50,
        "banana": 0.75,
        "orange": 1.25,
    }
)

category_resolver = DictResolver(
    {
        "apple": "fruit",
        "banana": "fruit",
        "orange": "fruit",
    }
)

engine = Engine()
engine.register_resolver("prices", price_resolver)
engine.register_resolver("categories", category_resolver)


# %%
# Use Lookups Alongside Expressions
# ---------------------------------
#
# The ``lookup`` helper operates on a pandas Series and a resolver. We can
# perform lookups in Python first, then use df-eval expressions to derive
# additional columns.

prices = lookup(sales_df["product"], price_resolver, on_missing="null")
categories = lookup(sales_df["product"], category_resolver, on_missing="null")

sales_with_lookups = sales_df.assign(price=prices, category=categories)

schema = {
    "line_total": "price * quantity",
}

result = engine.apply_schema(sales_with_lookups, schema)
result


# %%
# Handling Missing Keys
# ---------------------
#
# Control how missing keys are handled using the optional *on_missing*
# argument. Here we deliberately include an unknown product and ask
# ``lookup`` to return ``None`` for missing values.

sales_with_unknown = pd.DataFrame(
    {"product": ["apple", "banana", "unknown"], "quantity": [5, 7, 3]}
)

prices_missing = lookup(sales_with_unknown["product"], price_resolver, on_missing="null")

sales_with_unknown = sales_with_unknown.assign(price=prices_missing)

result_missing = engine.apply_schema(sales_with_unknown, {"line_total": "price * quantity"})
result_missing


