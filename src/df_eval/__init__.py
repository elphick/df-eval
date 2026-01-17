"""
df-eval: A lightweight expression evaluation engine for pandas DataFrames.

This package provides tools for evaluating expressions on pandas DataFrames,
supporting schema-driven derived columns and external lookups.
"""

__version__ = "0.1.0"

from df_eval.engine import Engine
from df_eval.expr import Expression

__all__ = ["Engine", "Expression", "__version__"]
