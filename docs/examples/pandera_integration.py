"""
Pandera Schema Integration
==========================

This example shows a schema-driven flow where Pandera handles validation/coercion
and df-eval derives expression-based columns from Pandera metadata.

Pipeline:

- Input DataFrame
- Pre-validation of input columns with Pandera
- Derived column evaluation with df-eval
- Optional full-schema post-validation
"""

from pprint import pprint

import pandas as pd
import pandera.pandas as pa

from df_eval import Engine
from df_eval.pandera import apply_pandera_schema, df_eval_schema_from_pandera


def _print_schema_preview(schema: pa.DataFrameSchema) -> None:
    # Build a compact, stable view so gallery output wraps across lines.
    schema_preview = {
        name: {
            "dtype": str(col.dtype),
            "checks": [str(check) for check in (col.checks or [])],
            "metadata": col.metadata or {},
        }
        for name, col in schema.columns.items()
    }
    print("Schema preview:")
    pprint(schema_preview, sort_dicts=False, width=100)

# %%
# Define Pandera Schema
# ---------------------

schema = pa.DataFrameSchema(
    {
        "value": pa.Column(float, coerce=True),
        "weight": pa.Column(float, coerce=True),
        "weighted": pa.Column(
            float,
            coerce=True,
            metadata={"df-eval": {"expr": "value * weight"}},
            checks=pa.Check.ge(0),
        ),
        "double_weighted": pa.Column(
            float,
            coerce=True,
            metadata={"df-eval": {"expr": "weighted * 2"}},
        ),
    }
)

_print_schema_preview(schema)

# %%
# Create DataFrame
# ----------------
# Input values are strings to demonstrate Pandera coercion.

df = pd.DataFrame({"value": ["10", "20", "30"], "weight": ["0.5", "0.25", "0.1"]})
df

# %%
# Inspect extracted expressions
# -----------------------------

expr_map = df_eval_schema_from_pandera(schema)
print("Extracted df-eval expressions:")
print(expr_map)

# %%
# Engine-first entry point
# ------------------------

engine = Engine()
result = engine.apply_pandera_schema(df, schema, coerce=True, validate=True, validate_post=True)
result

# %%
# Equivalent functional helper
# ----------------------------

result_via_helper = apply_pandera_schema(
    df,
    schema,
    coerce=True,
    validate=True,
    validate_post=True,
)
result_via_helper

