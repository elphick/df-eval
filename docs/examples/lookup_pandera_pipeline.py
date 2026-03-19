"""
Lookup and Function Pipeline via Pandera
========================================

This example shows how to drive a small pipeline entirely from Pandera
metadata, using df-eval for both lookup-style operations and generic
pipeline functions.

Pipeline:

- Define a Pandera schema with df-eval metadata
- Register a resolver for prices and a pipeline function for discounts
- Use ``apply_pandera_schema`` to validate, derive, and re-validate
"""

import pandas as pd
import pandera.pandas as pa

from df_eval import Engine, DictResolver
from df_eval.pandera import apply_pandera_schema


# %%
# Define Pandera Schema with df-eval Metadata
# -------------------------------------------
#
# ``price`` is driven by a lookup spec that references a named resolver.
# ``discounted_total`` is driven by a generic function step that calls a
# registered pipeline function.


schema = pa.DataFrameSchema(
    {
        "product": pa.Column(str),
        "quantity": pa.Column(int),
        "price": pa.Column(
            float,
            metadata={
                "df-eval": {
                    "lookup": {
                        "resolver": "prices",  # name of registered resolver
                        "key": "product",
                        "on_missing": "null",
                    }
                }
            },
        ),
        "line_total": pa.Column(
            float,
            metadata={"df-eval": {"expr": "price * quantity"}},
        ),
        "discounted_total": pa.Column(
            float,
            metadata={
                "df-eval": {
                    "function": {
                        "name": "apply_discount",     # registered pipeline function
                        "inputs": ["line_total"],
                        "outputs": ["discounted_total"],
                        "params": {"rate": 0.1},      # 10% discount
                    }
                }
            },
        ),
    }
)

schema


# %%
# Build Input Data
# ----------------


df = pd.DataFrame(
    {
        "product": ["apple", "banana", "orange"],
        "quantity": [10, 20, 15],
    }
)

df


# %%
# Register Resolver and Pipeline Function
# ---------------------------------------
#
# The resolver provides prices by product, while the pipeline function
# applies a simple percentage discount to ``line_total``.


price_resolver = DictResolver(
    {
        "apple": 1.50,
        "banana": 0.75,
        "orange": 1.25,
    }
)


def apply_discount(df_slice: pd.DataFrame, *, rate: float) -> pd.Series:
    """Simple example pipeline function applying a discount to a column.

    Expects a single input column ``line_total`` and returns the
    discounted total as a Series.
    """
    return df_slice["line_total"] * (1 - rate)


engine = Engine()
engine.register_resolver("prices", price_resolver)
engine.register_pipeline_function("apply_discount", apply_discount)


# %%
# Apply Pandera Schema
# --------------------
#
# ``apply_pandera_schema`` will:
#
# - Pre-validate input columns (excluding derived ones)
# - Apply df-eval-derived columns (including lookup + function steps)
# - Optionally re-validate the full schema


result = engine.apply_pandera_schema(df, schema, coerce=True, validate=True, validate_post=True)
result


