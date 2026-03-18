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
from df_eval.pandera import (
    df_eval_schema_from_pandera,
    apply_pandera_schema,
    apply_pandera_schema_parquet_to_parquet,
)
from df_eval.parquet import iter_parquet_row_chunks, write_parquet_row_chunks

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
    "df_eval_schema_from_pandera",
    "apply_pandera_schema",
    "apply_pandera_schema_parquet_to_parquet",
    "iter_parquet_row_chunks",
    "write_parquet_row_chunks",
    "__version__",
]
