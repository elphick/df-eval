"""Error Handling and Debugging
===============================

This example demonstrates how df-eval reports common errors and how you
can use provenance information to debug complex schemas.

It covers:

- Handling missing columns and invalid expressions
- Inspecting exceptions raised by the engine
- Enabling provenance and printing a simple lineage tree
"""

from pprint import pprint

import pandas as pd

from df_eval import Engine


def _print_lineage(provenance: dict, column_name: str, level: int = 0) -> None:
    """Recursively print a simple provenance tree for ``column_name``."""

    if column_name not in provenance:
        return

    info = provenance[column_name]
    indent = "  " * level
    expr = info.get("expression", "<unknown>")
    print(f"{indent}{column_name} = {expr}")
    for dep in info.get("dependencies", []):
        _print_lineage(provenance, dep, level + 1)


# %%
# Setup Sample Data
# -----------------

df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
df


# %%
# Handling Missing Columns
# ------------------------

engine = Engine()

try:
    # This will fail - column "z" does not exist
    engine.evaluate(df, "z + 1")
except Exception as exc:  # noqa: BLE001 - show raw exception for docs
    print("Caught exception for missing column:")
    print(type(exc).__name__, "-", exc)


# %%
# Handling Invalid Operations
# ---------------------------

df_invalid = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

try:
    # This may raise due to incompatible types in the expression
    engine.evaluate(df_invalid, "a + b")
except Exception as exc:  # noqa: BLE001 - show raw exception for docs
    print("Caught exception for invalid operation:")
    print(type(exc).__name__, "-", exc)


# %%
# Using Provenance for Debugging
# ------------------------------

engine.enable_provenance(True)

schema = {
    "derived1": "a + b",
    "derived2": "derived1 * 2",
}

result = engine.apply_schema(df, schema)

provenance = result.attrs.get("df_eval_provenance", {})
print("Provenance dictionary (truncated for display):")
pprint(provenance, sort_dicts=False)

print("\nLineage for 'derived2':")
_print_lineage(provenance, "derived2")

