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


def _classify_aliases(
    aliases: dict[str, list[str]],
    df_columns: set[str],
) -> dict[str, tuple[str, str]]:
    """Classify each alias into an explicit state based on what is in the DataFrame.

    Args:
        aliases: Mapping of target column name to list of candidate source names.
        df_columns: Set of column names currently present in the DataFrame.

    Returns:
        Mapping of target name to ``(kind, matched_source)`` where ``kind`` is one of:

        - ``"RENAME"``    — exactly one source present, target absent; rename source → target.
        - ``"NATIVE"``    — target present, no source present; alias not needed.
        - ``"AMBIGUOUS"`` — target and ≥1 source both present, or >1 sources present.
        - ``"ABSENT"``    — neither target nor any source present.

        ``matched_source`` is the single relevant source column for RENAME and AMBIGUOUS,
        or an empty string for NATIVE and ABSENT.
    """
    result: dict[str, tuple[str, str]] = {}
    for target, sources in aliases.items():
        present_sources = [s for s in sources if s in df_columns]
        target_present = target in df_columns

        if len(present_sources) > 1:
            result[target] = ("AMBIGUOUS", present_sources[0])
        elif len(present_sources) == 1 and target_present:
            result[target] = ("AMBIGUOUS", present_sources[0])
        elif len(present_sources) == 1 and not target_present:
            result[target] = ("RENAME", present_sources[0])
        elif target_present:
            result[target] = ("NATIVE", "")
        else:
            result[target] = ("ABSENT", "")
    return result


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
) -> dict[str, list[str]]:
    """Build alias mapping of target column -> list of candidate source columns.

    Accepts either a single string or a list of strings for the ``alias`` key::

        {"alias": "deposit"}           # single source — backward compatible
        {"alias": ["deposit", "dep"]}  # multiple candidates — tried in order
    """
    aliases: dict[str, list[str]] = {}
    operation_keys = ("expr", "lookup", "function")
    for col_name, section in _iter_df_eval_sections(df_schema, meta_key):
        if "alias" not in section:
            continue

        alias = section["alias"]
        if isinstance(alias, str):
            sources: list[str] = [alias]
        elif isinstance(alias, list):
            if not alias:
                raise ValueError(
                    f"metadata['{meta_key}']['alias'] for column '{col_name}' must not be empty"
                )
            if not all(isinstance(s, str) for s in alias):
                raise TypeError(
                    f"metadata['{meta_key}']['alias'] for column '{col_name}' "
                    "list must contain only strings"
                )
            sources = list(alias)
        else:
            raise TypeError(
                f"metadata['{meta_key}']['alias'] for column '{col_name}' "
                "must be a string or list of strings"
            )

        if any(key in section for key in operation_keys):
            raise ValueError(
                f"metadata['{meta_key}'] for column '{col_name}' cannot define both "
                "'alias' and an operation key ('expr', 'lookup', or 'function')"
            )
        aliases[col_name] = sources
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


def _extract_drop_columns(
    df_schema: Any,
    *,
    meta_key: str,
) -> set[str]:
    """Return schema columns marked for final-output suppression."""
    drop_columns: set[str] = set()
    for col_name, section in _iter_df_eval_sections(df_schema, meta_key):
        drop = section.get("drop")
        if drop is None:
            continue
        if not isinstance(drop, bool):
            raise TypeError(
                f"metadata['{meta_key}']['drop'] for column '{col_name}' must be a boolean"
            )
        if drop:
            drop_columns.add(col_name)
    return drop_columns


def _extract_ordered_categories(
    df_schema: Any,
    *,
    meta_key: str,
) -> dict[str, list[Any]]:
    """Return ordered categorical declarations keyed by column name."""
    ordered_categories: dict[str, list[Any]] = {}
    for col_name, section in _iter_df_eval_sections(df_schema, meta_key):
        ordered = section.get("ordered")
        if ordered is None:
            continue
        if not isinstance(ordered, bool):
            raise TypeError(
                f"metadata['{meta_key}']['ordered'] for column '{col_name}' must be a boolean"
            )
        if not ordered:
            continue

        checks = getattr(df_schema.columns[col_name], "checks", None)
        if checks is None:
            check_iterable: tuple[Any, ...] = ()
        elif isinstance(checks, (list, tuple)):
            check_iterable = tuple(checks)
        else:
            check_iterable = (checks,)

        categories: list[Any] | None = None
        for check in check_iterable:
            statistics = getattr(check, "statistics", None) or {}
            allowed_values = statistics.get("allowed_values")
            if allowed_values is not None:
                categories = list(allowed_values)
                break

        if categories is None:
            raise ValueError(
                f"metadata['{meta_key}']['ordered'] for column '{col_name}' "
                "requires a pandera Check.isin(...) so category order is unambiguous"
            )

        ordered_categories[col_name] = categories
    return ordered_categories


def _build_post_validation_exclusions(
    df_schema: Any,
    *,
    result_columns: set[str],
    aliases: dict[str, list[str]],
) -> set[str]:
    """Return absent schema columns that should be excluded from post-validation."""
    alias_source_columns = {
        source_col
        for source_cols in aliases.values()
        for source_col in source_cols
        if source_col in df_schema.columns
    }
    excluded_columns: set[str] = set()
    for col_name, col_schema in df_schema.columns.items():
        if col_name in result_columns:
            continue
        if not getattr(col_schema, "required", True) or col_name in alias_source_columns:
            excluded_columns.add(col_name)
    return excluded_columns


def _build_final_output_columns(
    result: pd.DataFrame,
    *,
    schema_column_order: list[str],
    drop_columns: set[str],
) -> list[str]:
    """Return the final output column order after applying drop metadata."""
    if not drop_columns:
        return list(result.columns)

    schema_column_set = set(schema_column_order)
    schema_columns = [
        col_name
        for col_name in schema_column_order
        if col_name in result.columns and col_name not in drop_columns
    ]
    extra_columns = [
        col_name
        for col_name in result.columns
        if col_name not in schema_column_set and col_name not in drop_columns
    ]
    return schema_columns + extra_columns


def validate_df_eval_schema(schema: Any, meta_key: str = "df-eval") -> None:
    """Validate the df-eval metadata embedded in a Pandera schema.

    Checks structural consistency rules that cannot be enforced at the
    individual-column level by ``_extract_aliases`` alone.

    Raises:
        TypeError: If ``schema`` is not a recognised Pandera schema type.
        ValueError: If any of the following are detected:

            - An alias target column is the same as one of its own sources
              (self-alias).
            - Two different alias targets claim the same source column
              (duplicate source).
            - An alias source column does not exist as a named column in the
              schema (unknown source).
    """
    pa = _import_pandera()
    df_schema = _to_dataframe_schema(schema, pa)
    schema_columns = set(df_schema.columns)

    aliases = _extract_aliases(df_schema, meta_key=meta_key)

    # Self-alias check: target must not appear in its own source list.
    for target, sources in aliases.items():
        if target in sources:
            raise ValueError(
                f"alias target '{target}' cannot list itself as a source"
            )

    # Duplicate-source check: each source may only belong to one target.
    source_to_targets: dict[str, list[str]] = {}
    for target, sources in aliases.items():
        for src in sources:
            source_to_targets.setdefault(src, []).append(target)
    duplicates = {
        src: targets
        for src, targets in source_to_targets.items()
        if len(targets) > 1
    }
    if duplicates:
        msgs = "; ".join(
            f"'{src}' claimed by {targets}" for src, targets in sorted(duplicates.items())
        )
        raise ValueError(f"alias sources must be unique across targets: {msgs}")

    # Unknown-source check: every source must be a named schema column.
    for target, sources in aliases.items():
        for src in sources:
            if src not in schema_columns:
                raise ValueError(
                    f"alias source '{src}' for target '{target}' "
                    f"is not a column defined in the schema"
                )


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
    """Apply alias transforms from Pandera metadata before operation evaluation.

    For each alias defined in the schema, copies the source column to the target
    when the source is present and the target is absent (RENAME).  When the target
    is already present (NATIVE) the alias is a no-op.  When neither is present
    (ABSENT) the alias is skipped silently.  When both are present (AMBIGUOUS)
    a ValueError is raised.

    The source column is *not* removed so that callers that depend on the source
    name (e.g. post-validation with ``strict: filter``) continue to work.
    """
    pa = _import_pandera()
    df_schema = _to_dataframe_schema(schema, pa)
    aliases = _extract_aliases(df_schema, meta_key=meta_key)

    if not aliases:
        return df

    classified = _classify_aliases(aliases, set(df.columns))
    result = df.copy()

    for target_col, (kind, source_col) in classified.items():
        if kind == "RENAME":
            result[target_col] = result[source_col]
        elif kind == "NATIVE":
            pass  # target already present — alias not needed
        elif kind == "AMBIGUOUS":
            raise ValueError(
                "input DataFrame contains both alias target and source columns: "
                f"'{target_col}' and '{source_col}'"
            )
        # ABSENT — skip silently

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


def _apply_ordered_categoricals(
    df: pd.DataFrame,
    schema: Any,
    *,
    meta_key: str = "df-eval",
) -> pd.DataFrame:
    """Recast columns declared as ordered categoricals in df-eval metadata."""
    pa = _import_pandera()
    df_schema = _to_dataframe_schema(schema, pa)
    ordered_categories = _extract_ordered_categories(df_schema, meta_key=meta_key)

    if not ordered_categories:
        return df

    result = df.copy()
    for col_name, categories in ordered_categories.items():
        if col_name not in result.columns:
            continue
        invalid_values = result.loc[
            result[col_name].notna() & ~result[col_name].isin(categories),
            col_name,
        ]
        if not invalid_values.empty:
            invalid_text = ", ".join(map(str, pd.unique(invalid_values)))
            raise ValueError(
                f"column '{col_name}' contains values outside its ordered category list: "
                f"{invalid_text}"
            )
        result[col_name] = result[col_name].astype(
            pd.CategoricalDtype(categories=categories, ordered=True)
        )
    return result


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
    parquet_columns: set[str] | None = None,
) -> tuple[dict[str, str], list[str], list[str], dict[str, str]]:
    """Return expression map, input projection, effective outputs, and copy map.

    When ``parquet_columns`` is provided, aliases are classified against the
    parquet file's column names.  RENAME aliases have their source names
    substituted into ``input_columns`` (the projection that reads only what the
    file actually contains).  The returned ``output_columns`` is restricted to
    schema columns that are achievable from the file.

    Args:
        schema: Pandera schema to plan from.
        meta_key: Metadata section key.
        expr_key: Expression key within the metadata section.
        parquet_columns: Optional set of column names present in the parquet file.
            When provided, alias resolution is file-aware.

    Returns:
        A four-tuple of ``(expr_map, input_columns, output_columns, copy_map)``
        where ``copy_map`` maps alias source names (as they appear in the file)
        → target names for any RENAME aliases, and ``input_columns`` uses source
        names so the projection matches what the file contains.
    """
    pa = _import_pandera()
    df_schema = _to_dataframe_schema(schema, pa)

    schema_output_columns = list(df_schema.columns)
    expr_map = df_eval_schema_from_pandera(df_schema, meta_key=meta_key, expr_key=expr_key)
    derived_columns = set(expr_map)
    drop_columns = _extract_drop_columns(df_schema, meta_key=meta_key)

    aliases = _extract_aliases(df_schema, meta_key=meta_key)

    # Classify aliases when we know the parquet schema up-front.
    # copy_map: source_in_file → target_in_schema  (for RENAME aliases)
    copy_map: dict[str, str] = {}
    if parquet_columns is not None and aliases:
        classified = _classify_aliases(aliases, parquet_columns)
        for target, (kind, source) in classified.items():
            if kind == "RENAME":
                copy_map[source] = target  # read source, copy to target
            elif kind == "AMBIGUOUS":
                raise ValueError(
                    "parquet file contains both alias target and source columns: "
                    f"'{target}' and '{source}'"
                )

    # target_to_source maps schema column names back to their file-level names
    # for RENAME aliases (inverse of copy_map).
    target_to_file: dict[str, str] = {tgt: src for src, tgt in copy_map.items()}

    required_input_columns = {
        column_name
        for column_name in schema_output_columns
        if column_name not in derived_columns
    }

    for expr in expr_map.values():
        dependencies = Expression(expr).dependencies
        required_input_columns.update(
            dependency
            for dependency in dependencies
            if dependency in df_schema.columns and dependency not in derived_columns
        )

    # Build file-level input_columns: each schema column maps to its file name
    # via target_to_file (for RENAME aliases) or stays as-is.  Deduplicate while
    # preserving order, and skip columns not present in the file when
    # parquet_columns is known.
    seen: set[str] = set()
    input_columns: list[str] = []
    for col in schema_output_columns:
        if col not in required_input_columns:
            continue
        file_col = target_to_file.get(col, col)
        if file_col in seen:
            continue
        if parquet_columns is not None and file_col not in parquet_columns:
            continue
        input_columns.append(file_col)
        seen.add(file_col)

    # Effective output columns: restrict to schema columns that are achievable.
    # A column is achievable if it is derivable, directly in the file, or is a
    # RENAME target whose source is in the file.
    if parquet_columns is not None:
        output_columns = [
            col for col in schema_output_columns
            if col in derived_columns
            or col in parquet_columns
            or target_to_file.get(col, "") in parquet_columns
        ]
    else:
        output_columns = schema_output_columns

    output_columns = [
        col_name for col_name in output_columns if col_name not in drop_columns
    ]

    return expr_map, input_columns, output_columns, copy_map


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

    1. Classify aliases against the input DataFrame columns.
    2. Raise on AMBIGUOUS aliases; copy RENAME aliases (source → target) so that
       alias targets are present before Pandera sees the DataFrame.
    3. Pre-validate base input columns (excluding derived columns not yet
       present, plus alias source columns that are absent for optional alias
       fallback behavior).
    4. Apply decimals transforms for existing columns.
    5. Apply df-eval operations.
    6. Optional post-validation against the full schema, excluding only absent
       optional columns and absent alias-source columns.
    7. Recast any columns marked ``ordered=True`` using the order declared by
       their Pandera ``Check.isin(...)`` values.
    8. If any columns are marked ``drop=True``, remove them from the final output
       using schema order.

    The df-eval metadata for each operation column may define one of:

    ``{"expr": "a + b"}``
    ``{"lookup": {"resolver": "prices", "key": "product"}}``
    ``{"function": {"name": "my_fn", "inputs": ["a"], "outputs": ["y"]}}``

    Any operation may include ``"decimals": <int>`` to round the derived output.
    Decimals are also applied to alias targets before operations run.
    """
    pa = _import_pandera()
    df_schema = _to_dataframe_schema(schema, pa)
    eval_engine = engine or Engine()

    operations = df_eval_operations_from_pandera(df_schema, meta_key=meta_key)
    aliases = _extract_aliases(df_schema, meta_key=meta_key)
    drop_columns = _extract_drop_columns(df_schema, meta_key=meta_key)
    schema_column_order = list(df_schema.columns)
    schema_columns = set(df_schema.columns)
    derived_columns = set(operations)

    # Classify aliases and materialise RENAME copies BEFORE Pandera sees the df.
    # This ensures that strict:filter cannot drop a source column we still need,
    # and that alias targets are immediately available for validation.
    classified = _classify_aliases(aliases, set(df.columns))
    for target_col, (kind, source_col) in classified.items():
        if kind == "AMBIGUOUS":
            raise ValueError(
                "input DataFrame contains both alias target and source columns: "
                f"'{target_col}' and '{source_col}'"
            )

    # Build the DataFrame with alias copies applied.
    df_with_aliases = df.copy()
    for target_col, (kind, source_col) in classified.items():
        if kind == "RENAME":
            df_with_aliases[target_col] = df_with_aliases[source_col]

    # Exclude from pre-validation:
    # 1) derived columns that are not yet computed, and
    # 2) missing alias *source* columns (alias targets remain required).
    # This preserves optional alias-source behavior while ensuring missing
    # non-derived schema columns still fail validation.
    df_with_aliases_cols = set(df_with_aliases.columns)
    missing_alias_sources = {
        source_col
        for source_cols in aliases.values()
        for source_col in source_cols
        if source_col in schema_columns and source_col not in df_with_aliases_cols
    }
    pre_validation_excluded_columns = missing_alias_sources | {
        col for col in derived_columns if col not in df_with_aliases_cols
    }

    validated_df = df_with_aliases
    if validate:
        base_schema = _build_subset_schema(df_schema, pre_validation_excluded_columns)
        validated_df = _validate_with_coerce(base_schema, df_with_aliases, coerce=coerce)

    transformed_df = _apply_decimals_with_engine(
        validated_df,
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
        result_cols = set(result.columns)
        post_validation_excluded_columns = _build_post_validation_exclusions(
            df_schema,
            result_columns=result_cols,
            aliases=aliases,
        )
        post_validation_schema = _build_subset_schema(
            df_schema, post_validation_excluded_columns
        )
        result = _validate_with_coerce(post_validation_schema, result, coerce=coerce)

    result = _apply_ordered_categoricals(result, df_schema, meta_key=meta_key)

    final_output_columns = _build_final_output_columns(
        result,
        schema_column_order=schema_column_order,
        drop_columns=drop_columns,
    )
    return result.loc[:, final_output_columns]


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

    The input scan is projected to only required source columns (using alias
    source names when the parquet file contains them).  RENAME aliases are
    applied per-chunk as copies (source column is preserved in output when it
    is a named schema column).  Output columns are restricted to schema columns
    that are achievable from the file.

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
    from df_eval.parquet import iter_parquet_row_chunks, write_parquet_row_chunks

    input_path = Path(input_path).expanduser()

    # Read parquet schema without loading data to classify aliases up-front.
    pq = _import_pyarrow_parquet()
    parquet_columns = set(pq.read_schema(str(input_path)).names)

    expr_map, input_columns, output_columns, copy_map = _plan_pandera_parquet_projection(
        schema,
        meta_key=meta_key,
        expr_key=expr_key,
        parquet_columns=parquet_columns,
    )

    eval_engine = engine or Engine()

    def _transformed_chunks() -> Any:
        for chunk in iter_parquet_row_chunks(
            input_path,
            chunk_size=chunk_size,
            columns=input_columns,
        ):
            # Copy alias sources to their target names before expression evaluation.
            # The source column is kept so that it can appear in the output when it
            # is a named schema column.
            for file_col, target_col in copy_map.items():
                chunk[target_col] = chunk[file_col]
            transformed = eval_engine.apply_schema(chunk, expr_map)
            yield transformed[output_columns]

    return write_parquet_row_chunks(
        _transformed_chunks(),
        output_path,
        compression=compression,
    )


def _import_pyarrow_parquet() -> Any:
    """Import pyarrow.parquet lazily for schema inspection."""
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ImportError(
            "Parquet helpers require the optional dependency. "
            "Install with: pip install 'df-eval[parquet]'"
        ) from exc
    return pq


__all__ = [
    "df_eval_schema_from_pandera",
    "apply_aliases",
    "apply_decimals",
    "apply_pandera_schema",
    "apply_pandera_schema_parquet_to_parquet",
    "df_eval_operations_from_pandera",
    "validate_df_eval_schema",
    "load_pandera_schema_yaml",
    "dump_pandera_schema_yaml",
    "load_pandera_schema_json",
    "dump_pandera_schema_json",
]
