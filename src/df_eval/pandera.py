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


def _iter_df_eval_sections(df_schema: Any, meta_key: str) -> list[tuple[str, Mapping[str, Any]]]:
    """Return validated per-column df-eval metadata sections."""
    sections: list[tuple[str, Mapping[str, Any]]] = []
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
        sections.append((col_name, section))
    return sections


def _extract_aliases(
    df_schema: Any,
    *,
    meta_key: str,
) -> dict[str, str]:
    """Build alias mapping of target column -> source column."""
    aliases: dict[str, str] = {}
    operation_keys = ("expr", "lookup", "function")
    for col_name, section in _iter_df_eval_sections(df_schema, meta_key):
        if "alias" not in section:
            continue

        alias = section["alias"]
        if not isinstance(alias, str):
            raise TypeError(
                f"metadata['{meta_key}']['alias'] for column '{col_name}' must be a string"
            )
        if any(key in section for key in operation_keys):
            raise ValueError(
                f"metadata['{meta_key}'] for column '{col_name}' cannot define both "
                "'alias' and an operation key ('expr', 'lookup', or 'function')"
            )
        aliases[col_name] = alias
    return aliases


def _extract_decimals(
    df_schema: Any,
    *,
    meta_key: str,
) -> dict[str, int]:
    """Build decimals mapping for any column that defines transform rounding."""
    decimals_map: dict[str, int] = {}
    for col_name, section in _iter_df_eval_sections(df_schema, meta_key):
        decimals = section.get("decimals")
        if decimals is None:
            continue
        if not isinstance(decimals, int):
            raise TypeError(
                f"metadata['{meta_key}']['decimals'] for column '{col_name}' must be an integer"
            )
        decimals_map[col_name] = decimals
    return decimals_map


def df_eval_schema_from_pandera(
    schema: Any,
    meta_key: str = "df-eval",
    expr_key: str = "expr",
) -> dict[str, str]:
    """Build a df-eval schema mapping from Pandera per-column metadata."""
    pa = _import_pandera()
    df_schema = _to_dataframe_schema(schema, pa)

    expr_map: dict[str, str] = {}
    for col_name, section in _iter_df_eval_sections(df_schema, meta_key):
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

    Any operation may also include an optional rounding directive::

        {"expr": "price * quantity", "decimals": 2}

    The returned mapping has the shape::

        {
            "column_name": {
                "kind": "expr" | "lookup" | "function",
                "expr": str | None,
                "lookup": dict | None,
                "function": dict | None,
                "decimals": int | None,
            },
        }
    """
    pa = _import_pandera()
    df_schema = _to_dataframe_schema(schema, pa)

    ops: dict[str, dict[str, Any]] = {}
    for col_name, section in _iter_df_eval_sections(df_schema, meta_key):
        decimals = section.get("decimals")
        if decimals is not None and not isinstance(decimals, int):
            raise TypeError(
                f"metadata['{meta_key}']['decimals'] for column '{col_name}' must be an integer"
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
                "decimals": decimals,
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
                "decimals": decimals,
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
                "decimals": decimals,
            }

    return ops


def apply_aliases(
    df: pd.DataFrame,
    schema: Any,
    meta_key: str = "df-eval",
) -> pd.DataFrame:
    """Apply alias transforms from Pandera metadata before operation evaluation."""
    pa = _import_pandera()
    df_schema = _to_dataframe_schema(schema, pa)
    aliases = _extract_aliases(df_schema, meta_key=meta_key)

    if not aliases:
        return df

    result = df.copy()
    for target_col, source_col in aliases.items():
        source_exists = source_col in result.columns
        target_exists = target_col in result.columns

        if target_exists and source_exists and target_col != source_col:
            raise ValueError(
                "input DataFrame contains both alias target and source columns: "
                f"'{target_col}' and '{source_col}'"
            )
        if not target_exists and not source_exists:
            raise ValueError(
                f"cannot apply alias for '{target_col}': source column '{source_col}' is missing"
            )
        if not target_exists and source_exists:
            result[target_col] = result[source_col]

    return result


def _apply_decimals_with_engine(
    df: pd.DataFrame,
    schema: Any,
    *,
    meta_key: str = "df-eval",
    engine: Engine | None = None,
) -> pd.DataFrame:
    """Apply decimals transform to existing DataFrame columns only."""
    pa = _import_pandera()
    df_schema = _to_dataframe_schema(schema, pa)
    decimals_map = _extract_decimals(df_schema, meta_key=meta_key)

    if not decimals_map:
        return df

    result = df.copy()
    eval_engine = engine or Engine()
    for col_name, decimals in decimals_map.items():
        if col_name not in result.columns:
            continue
        result[col_name] = eval_engine._apply_rounding_if_requested(
            result[col_name],
            decimals,
        )
    return result


def apply_decimals(
    df: pd.DataFrame,
    schema: Any,
    meta_key: str = "df-eval",
) -> pd.DataFrame:
    """Apply decimals transform to existing columns using Pandera metadata."""
    return _apply_decimals_with_engine(df, schema, meta_key=meta_key)


def load_pandera_schema_yaml(source: str | Path) -> Any:
    """Load a Pandera DataFrameSchema from YAML, preserving column and schema metadata.

    Uses df-eval's own schema serialization, which preserves the ``metadata``
    field at both the column and the dataframe level through a full IO round-trip.

    Args:
        source: Path to a YAML schema file or a YAML string.

    Returns:
        A Pandera :class:`~pandera.api.pandas.container.DataFrameSchema`.
    """
    _import_pandera()  # ensure the optional dependency is present with a clear error
    from df_eval.utils import pandera_io_compat as _pa_io

    return _pa_io.from_yaml(source)


def dump_pandera_schema_yaml(schema: Any, stream: str | Path | None = None) -> str | None:
    """Dump a Pandera DataFrameSchema to YAML, preserving column and schema metadata.

    Uses df-eval's own schema serialization so that both column-level and
    dataframe-level ``metadata`` survive a full IO round-trip.

    Args:
        schema: A Pandera SchemaModel/DataFrameModel class or DataFrameSchema.
        stream: Optional path or file-like to write to. If ``None``, the
            YAML representation is returned as a string.

    Returns:
        The YAML string if ``stream`` is ``None``, otherwise ``None``.
    """
    _import_pandera()
    from df_eval.utils import pandera_io_compat as _pa_io

    return _pa_io.to_yaml(schema, stream=stream)


def load_pandera_schema_json(source: str | Path) -> Any:
    """Load a Pandera DataFrameSchema from JSON, preserving column and schema metadata.

    This mirrors :func:`load_pandera_schema_yaml` but for JSON input.
    """
    _import_pandera()
    from df_eval.utils import pandera_io_compat as _pa_io

    return _pa_io.from_json(source)


def dump_pandera_schema_json(schema: Any, target: str | Path | None = None, **kwargs: Any) -> str | None:
    """Dump a Pandera DataFrameSchema to JSON, preserving column and schema metadata.

    Args:
        schema: A Pandera SchemaModel/DataFrameModel class or DataFrameSchema.
        target: Optional path or file-like to write to. If ``None``, the
            JSON representation is returned as a string.
        **kwargs: Extra keyword arguments forwarded to :func:`json.dump`.
    """
    _import_pandera()
    from df_eval.utils import pandera_io_compat as _pa_io

    return _pa_io.to_json(schema, target=target, **kwargs)


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
    """Run the Pandera + df-eval pipeline and optionally post-validate.

    Pipeline order:

    1. pre-validate base input columns (excluding operation/alias targets)
    2. apply alias transforms from metadata
    3. apply decimals transforms for existing columns
    4. apply df-eval operations
    5. optional post-validation against the full schema

    The df-eval metadata for each operation column may define one of the
    following keys:

    ``{"expr": "a + b"}``
    ``{"lookup": {"resolver": "prices", "key": "product"}}``
    ``{"function": {"name": "my_fn", "inputs": ["a"], "outputs": ["y"]}}``

    Any operation may include ``"decimals": <int>`` to round the derived
    output. Transform-stage decimals are also supported for any column that
    already exists by stage (3), including aliased base columns.

    These are translated into an operations mapping consumed by
    :meth:`df_eval.engine.Engine.apply_operations`.
    """
    pa = _import_pandera()
    df_schema = _to_dataframe_schema(schema, pa)
    eval_engine = engine or Engine()

    operations = df_eval_operations_from_pandera(df_schema, meta_key=meta_key)
    aliases = _extract_aliases(df_schema, meta_key=meta_key)
    derived_columns = set(operations)
    pre_validation_excluded_columns = derived_columns.union(aliases.keys())

    validated_df = df
    if validate:
        base_schema = _build_subset_schema(df_schema, pre_validation_excluded_columns)
        validated_df = _validate_with_coerce(base_schema, df, coerce=coerce)
    transformed_df = apply_aliases(validated_df, df_schema, meta_key=meta_key)
    transformed_df = _apply_decimals_with_engine(
        transformed_df,
        df_schema,
        meta_key=meta_key,
        engine=eval_engine,
    )

    if error_on_overwrite:
        overlapping = derived_columns.intersection(transformed_df.columns)
        if overlapping:
            overlap_text = ", ".join(sorted(overlapping))
            raise ValueError(
                "input DataFrame already contains derived columns marked by Pandera "
                f"metadata: {overlap_text}"
            )

    result = transformed_df
    if operations:
        result = eval_engine.apply_operations(transformed_df, operations)

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
    "apply_aliases",
    "apply_decimals",
    "apply_pandera_schema",
    "apply_pandera_schema_parquet_to_parquet",
    "df_eval_operations_from_pandera",
    "load_pandera_schema_yaml",
    "dump_pandera_schema_yaml",
    "load_pandera_schema_json",
    "dump_pandera_schema_json",
]
