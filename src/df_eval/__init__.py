"""
df-eval: A lightweight expression evaluation engine for pandas DataFrames.

This package provides tools for evaluating expressions on pandas DataFrames,
supporting schema-driven derived columns and external lookups.
"""

__version__ = "0.1.0"

from df_eval.engine import Engine, CycleDetectedError
from df_eval.expr import Expression
from df_eval.lookup import (
    lookup,
    Resolver,
    CachedResolver,
    DictResolver,
    FileResolver,
    DatabaseResolver,
    HTTPResolver,
)

__all__ = [
    "Engine",
    "Expression",
    "CycleDetectedError",
    "lookup",
    "Resolver",
    "CachedResolver",
    "DictResolver",
    "FileResolver",
    "DatabaseResolver",
    "HTTPResolver",
    "__version__",
]
