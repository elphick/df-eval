"""Pandera integration helpers for df-eval.

This module keeps Pandera support optional and layered on top of the core
Engine API by translating Pandera column metadata into a df-eval schema map.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd

from df_eval.engine import Engine
from df_eval.expr import Expression


def _import_pandera() -> Any:
    """Import pandera lazily so df-eval works without the optional dependency."""
    try:
        import pandera as pa
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ImportError(
            "Pandera support requires the optional dependency. "
            "Install with: pip install 'df-eval[pandera]'"
        ) from exc
    return pa


def _is_schema_model_subclass(schema: Any, pa: Any) -> bool:
    """Return True when schema is a Pandera model class with to_schema()."""
    if not isinstance(schema, type):
        return False

    if callable(getattr(schema, "to_schema", None)):
        return True

    model_types = tuple(
        candidate
        for candidate in (
            getattr(pa, "SchemaModel", None),
            getattr(pa, "DataFrameModel", None),
        )
        if isinstance(candidate, type)
    )
    return bool(model_types) and issubclass(schema, model_types)


def _to_dataframe_schema(schema: Any, pa: Any) -> Any:
    """Normalize SchemaModel/DataFrameModel classes and DataFrameSchema objects."""
    if _is_schema_model_subclass(schema, pa):
        return schema.to_schema()

    has_schema_shape = (
        hasattr(schema, "columns")
        and callable(getattr(schema, "validate", None))
    )
    if has_schema_shape:
        return schema

    raise TypeError(
        "schema must be a pandera SchemaModel/DataFrameModel subclass "
        "or a pandera DataFrameSchema"
    )


def _build_subset_schema(df_schema: Any, excluded_columns: set[str]) -> Any:
    """Create a schema with selected columns removed for pre-validation."""
    if not excluded_columns:
        return df_schema

    try:
        return df_schema.remove_columns(list(excluded_columns))
    except AttributeError:
        columns = {
            name: column
            for name, column in df_schema.columns.items()
            if name not in excluded_columns
        }
        kwargs = {
            "index": getattr(df_schema, "index", None),
            "dtype": getattr(df_schema, "dtype", None),
            "coerce": getattr(df_schema, "coerce", False),
            "strict": getattr(df_schema, "strict", False),
            "name": getattr(df_schema, "name", None),
            "ordered": getattr(df_schema, "ordered", False),
            "unique": getattr(df_schema, "unique", None),
            "checks": getattr(df_schema, "checks", None),
        }
        kwargs = {key: value for key, value in kwargs.items() if value is not None}
        return df_schema.__class__(columns=columns, **kwargs)


def _validate_with_coerce(df_schema: Any, df: pd.DataFrame, coerce: bool) -> pd.DataFrame:
    """Validate across Pandera versions with/without validate(..., coerce=...)."""
    try:
        return df_schema.validate(df, coerce=coerce)
    except TypeError as exc:
        if "coerce" not in str(exc):
            raise

    # Newer Pandera versions removed the validate(..., coerce=...) kwarg.
    schema_copy = copy.deepcopy(df_schema)
    schema_copy.coerce = coerce
    for column in schema_copy.columns.values():
        column.coerce = coerce
    return schema_copy.validate(df)


def df_eval_schema_from_pandera(
    schema: Any,
    meta_key: str = "df-eval",
    expr_key: str = "expr",
) -> dict[str, str]:
    """Build a df-eval schema mapping from Pandera per-column metadata."""
    pa = _import_pandera()
    df_schema = _to_dataframe_schema(schema, pa)

    expr_map: dict[str, str] = {}
    for col_name, col_schema in df_schema.columns.items():
        metadata = col_schema.metadata or {}
        if not isinstance(metadata, Mapping):
            raise TypeError(f"metadata for column '{col_name}' must be a mapping")

        section = metadata.get(meta_key)
        if section is None:
            continue
        if not isinstance(section, Mapping):
            raise TypeError(
                f"metadata['{meta_key}'] for column '{col_name}' must be a mapping"
            )

        expr = section.get(expr_key)
        if expr is None:
            continue
        if not isinstance(expr, str):
            raise TypeError(
                f"metadata['{meta_key}']['{expr_key}'] for column '{col_name}' "
                "must be a string"
            )
        expr_map[col_name] = expr

    return expr_map


def df_eval_operations_from_pandera(
    schema: Any,
    meta_key: str = "df-eval",
) -> dict[str, dict[str, Any]]:
    """Build a rich df-eval operations mapping from Pandera column metadata.

    Each column may define one of the following under ``metadata[meta_key]``::

        {"expr": "a + b"}
        {"lookup": {"resolver": "prices", "key": "product"}}
        {"function": {"name": "churn_model_v1", "inputs": ["age"]}}

    The returned mapping has the shape::

        {
            "column_name": {
                "kind": "expr" | "lookup" | "function",
                "expr": str | None,
                "lookup": dict | None,
                "function": dict | None,
            },
        }
    """
    pa = _import_pandera()
    df_schema = _to_dataframe_schema(schema, pa)

    ops: dict[str, dict[str, Any]] = {}
    for col_name, col_schema in df_schema.columns.items():
        metadata = col_schema.metadata or {}
        if not isinstance(metadata, Mapping):
            continue

        section = metadata.get(meta_key)
        if section is None:
            continue
        if not isinstance(section, Mapping):
            raise TypeError(
                f"metadata['{meta_key}'] for column '{col_name}' must be a mapping"
            )

        if "expr" in section:
            expr = section["expr"]
            if not isinstance(expr, str):
                raise TypeError(
                    f"metadata['{meta_key}']['expr'] for column '{col_name}' must be a string"
                )
            ops[col_name] = {
                "kind": "expr",
                "expr": expr,
                "lookup": None,
                "function": None,
            }
        elif "lookup" in section:
            lookup_spec = section["lookup"]
            if not isinstance(lookup_spec, Mapping):
                raise TypeError(
                    f"metadata['{meta_key}']['lookup'] for column '{col_name}' must be a mapping"
                )
            ops[col_name] = {
                "kind": "lookup",
                "expr": None,
                "lookup": dict(lookup_spec),
                "function": None,
            }
        elif "function" in section:
            function_spec = section["function"]
            if not isinstance(function_spec, Mapping):
                raise TypeError(
                    f"metadata['{meta_key}']['function'] for column '{col_name}' must be a mapping"
                )
            ops[col_name] = {
                "kind": "function",
                "expr": None,
                "lookup": None,
                "function": dict(function_spec),
            }

    return ops


def _plan_pandera_parquet_projection(
    schema: Any,
    *,
    meta_key: str,
    expr_key: str,
) -> tuple[dict[str, str], list[str], list[str]]:
    """Return expression map, input projection, and schema-ordered outputs."""
    pa = _import_pandera()
    df_schema = _to_dataframe_schema(schema, pa)

    output_columns = list(df_schema.columns)
    expr_map = df_eval_schema_from_pandera(df_schema, meta_key=meta_key, expr_key=expr_key)
    derived_columns = set(expr_map)

    required_input_columns = {
        column_name
        for column_name in output_columns
        if column_name not in derived_columns
    }

    for expr in expr_map.values():
        dependencies = Expression(expr).dependencies
        required_input_columns.update(
            dependency
            for dependency in dependencies
            if dependency in df_schema.columns and dependency not in derived_columns
        )

    input_columns = [
        column_name
        for column_name in output_columns
        if column_name in required_input_columns
    ]
    return expr_map, input_columns, output_columns


def apply_pandera_schema(
    df: pd.DataFrame,
    schema: Any,
    *,
    meta_key: str = "df-eval",
    coerce: bool = True,
    validate: bool = True,
    validate_post: bool = True,
    engine: Engine | None = None,
    error_on_overwrite: bool = True,
) -> pd.DataFrame:
    """Validate with Pandera, apply df-eval operations, then optionally revalidate.

    Columns that define df-eval metadata under ``meta_key`` are considered derived
    and are excluded from pre-validation. This allows input frames that do not yet
    include derived columns.

    The df-eval metadata for each column may currently define exactly one of the
    following keys:

    ``{"expr": "a + b"}``
    ``{"lookup": {"resolver": "prices", "key": "product"}}``
    ``{"function": {"name": "my_fn", "inputs": ["a"], "outputs": ["y"]}}``

    These are translated into an operations mapping consumed by
    :meth:`df_eval.engine.Engine.apply_operations`.
    """
    pa = _import_pandera()
    df_schema = _to_dataframe_schema(schema, pa)

    # Build a rich operations map (expr, lookup, function) from column metadata.
    from df_eval.pandera import df_eval_operations_from_pandera

    operations = df_eval_operations_from_pandera(df_schema, meta_key=meta_key)
    derived_columns = set(operations)

    validated_df = df
    if validate:
        base_schema = _build_subset_schema(df_schema, derived_columns)
        validated_df = _validate_with_coerce(base_schema, df, coerce=coerce)

    # If there are no df-eval-driven columns, there is nothing for the engine to do.
    # Mirror previous behaviour and skip post-validation in this case.
    if not operations:
        return validated_df

    if error_on_overwrite:
        overlapping = derived_columns.intersection(validated_df.columns)
        if overlapping:
            overlap_text = ", ".join(sorted(overlapping))
            raise ValueError(
                "input DataFrame already contains derived columns marked by Pandera "
                f"metadata: {overlap_text}"
            )

    eval_engine = engine or Engine()
    result = eval_engine.apply_operations(validated_df, operations)

    if validate and validate_post:
        result = _validate_with_coerce(df_schema, result, coerce=coerce)

    return result


def apply_pandera_schema_parquet_to_parquet(
    input_path: str | Path,
    output_path: str | Path,
    schema: Any,
    *,
    meta_key: str = "df-eval",
    expr_key: str = "expr",
    engine: Engine | None = None,
    chunk_size: int = 100_000,
    compression: str = "snappy",
) -> Path:
    """Apply a Pandera-driven schema to Parquet input and write Parquet output.

    The input scan is projected to only required source columns, and output
    columns are restricted to the Pandera schema order.

    Args:
        input_path: Source Parquet file or directory-backed dataset.
        output_path: Destination Parquet file.
        schema: Pandera SchemaModel/DataFrameModel class or DataFrameSchema.
        meta_key: Metadata section containing df-eval expressions.
        expr_key: Metadata key containing the expression text.
        engine: Optional Engine instance.
        chunk_size: Maximum rows processed per chunk.
        compression: Parquet compression codec used for output.

    Returns:
        The normalized output path.
    """
    expr_map, input_columns, output_columns = _plan_pandera_parquet_projection(
        schema,
        meta_key=meta_key,
        expr_key=expr_key,
    )

    eval_engine = engine or Engine()
    return eval_engine.apply_schema_parquet_to_parquet(
        input_path,
        output_path,
        expr_map,
        chunk_size=chunk_size,
        input_columns=input_columns,
        output_columns=output_columns,
        compression=compression,
    )


__all__ = [
    "df_eval_schema_from_pandera",
    "apply_pandera_schema",
    "apply_pandera_schema_parquet_to_parquet",
    "df_eval_operations_from_pandera",
]
