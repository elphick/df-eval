"""Internal schema serialization for df-eval.

Implements YAML/JSON round-trips for Pandera DataFrameSchema while preserving
column- and dataframe-level metadata.  This module intentionally avoids
``pandera[io]``, ``pandera.schema_statistics``, and any other private Pandera
internals so that only ``pandera`` (no extras) and ``pyyaml`` are required.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "Schema IO requires 'pyyaml' to be installed.\n"
        "Install with: pip install pyyaml\n"
    ) from exc


# ---------------------------------------------------------------------------
# Internal helpers: Check serialization
# ---------------------------------------------------------------------------

def _check_statistics(check: Any) -> Any:
    """Extract check statistics using the public API where possible.

    Pandera >= 0.21 exposes `statistics` as a property; older versions stored
    them in the private `_statistics` attribute.
    """
    stats = getattr(check, "statistics", None)
    if stats is None:
        stats = getattr(check, "_statistics", None)
    return stats


def _serialize_checks(checks: list | None) -> dict | None:
    """Convert a list of Check objects to a JSON/YAML-compatible dict."""
    if not checks:
        return None
    result: dict[str, Any] = {}
    for check in checks:
        stats = _check_statistics(check)
        if stats is None:
            result[check.name] = None
        elif isinstance(stats, dict) and len(stats) == 1:
            # Unary check (e.g. ge(0)) — store scalar directly so YAML stays compact.
            result[check.name] = next(iter(stats.values()))
        else:
            result[check.name] = stats
    return result or None


def _deserialize_checks(checks_dict: dict | None) -> list | None:
    """Reconstruct Check objects from a serialized dict."""
    if not checks_dict:
        return None

    from pandera import Check  # local import keeps module optional-dep-free at collection time

    result = []
    for check_name, check_stats in checks_dict.items():
        factory = getattr(Check, check_name, None)
        if factory is None:
            raise ValueError(f"Unknown pandera Check: {check_name!r}")
        if check_stats is None:
            result.append(factory())
        elif isinstance(check_stats, dict):
            result.append(factory(**check_stats))
        else:
            result.append(factory(check_stats))
    return result or None


# ---------------------------------------------------------------------------
# Internal helpers: Column serialization
# ---------------------------------------------------------------------------

def _serialize_column(col: Any) -> dict:
    """Serialize a pandera Column to a plain dict."""
    dtype = col.dtype
    return {
        "dtype": str(dtype) if dtype is not None else None,
        "nullable": col.nullable,
        "required": col.required,
        "coerce": col.coerce,
        "unique": col.unique,
        "regex": getattr(col, "regex", False),
        "title": getattr(col, "title", None),
        "description": getattr(col, "description", None),
        "metadata": col.metadata or None,
        "checks": _serialize_checks(col.checks),
    }


def _deserialize_column(col_data: dict) -> Any:
    """Reconstruct a pandera Column from a serialized dict."""
    from pandera import Column  # lazy import

    dtype = col_data.get("dtype")
    checks = _deserialize_checks(col_data.get("checks"))

    kwargs: dict[str, Any] = {
        "nullable": col_data.get("nullable", False),
        "required": col_data.get("required", True),
        "coerce": col_data.get("coerce", False),
        "regex": col_data.get("regex", False),
    }
    if dtype is not None:
        kwargs["dtype"] = dtype
    if checks:
        kwargs["checks"] = checks
    for opt in ("unique", "title", "description", "metadata"):
        val = col_data.get(opt)
        if val is not None:
            kwargs[opt] = val

    return Column(**kwargs)


# ---------------------------------------------------------------------------
# Internal helpers: Index serialization
# ---------------------------------------------------------------------------

def _serialize_index_component(idx: Any) -> dict:
    """Serialize a single Index component to a plain dict."""
    dtype = idx.dtype
    return {
        "dtype": str(dtype) if dtype is not None else None,
        "nullable": idx.nullable,
        "coerce": idx.coerce,
        "unique": getattr(idx, "unique", None),
        "name": getattr(idx, "name", None),
        "title": getattr(idx, "title", None),
        "description": getattr(idx, "description", None),
        "metadata": getattr(idx, "metadata", None) or None,
        "checks": _serialize_checks(idx.checks),
    }


def _serialize_index(index: Any) -> list[dict] | None:
    """Serialize Index or MultiIndex to a list of dicts."""
    if index is None:
        return None
    if hasattr(index, "indexes"):
        # MultiIndex
        return [_serialize_index_component(i) for i in index.indexes]
    return [_serialize_index_component(index)]


def _deserialize_index(index_list: list[dict] | None) -> Any:
    """Reconstruct an Index or MultiIndex from a list of dicts."""
    if not index_list:
        return None

    from pandera import Index, MultiIndex  # lazy import

    def _build(idx_data: dict) -> Any:
        checks = _deserialize_checks(idx_data.get("checks"))
        kwargs: dict[str, Any] = {
            "nullable": idx_data.get("nullable", False),
            "coerce": idx_data.get("coerce", False),
        }
        dtype = idx_data.get("dtype")
        if dtype is not None:
            kwargs["dtype"] = dtype
        if checks:
            kwargs["checks"] = checks
        for opt in ("unique", "name", "title", "description", "metadata"):
            val = idx_data.get(opt)
            if val is not None:
                kwargs[opt] = val
        return Index(**kwargs)

    if len(index_list) == 1:
        return _build(index_list[0])
    return MultiIndex(indexes=[_build(d) for d in index_list])


# ---------------------------------------------------------------------------
# Public schema serialization
# ---------------------------------------------------------------------------

def schema_to_dict(schema: Any) -> dict:
    """Serialize a pandera DataFrameSchema to a JSON/YAML-compatible dict.

    Both column-level and dataframe-level ``metadata`` are preserved.
    """
    import pandera  # for version string

    columns = {
        name: _serialize_column(col)
        for name, col in (schema.columns or {}).items()
    }
    return {
        "schema_type": "dataframe",
        "version": pandera.__version__,
        "columns": columns or None,
        "checks": _serialize_checks(schema.checks) if schema.checks else None,
        "index": _serialize_index(schema.index),
        "coerce": schema.coerce,
        "strict": schema.strict,
        "name": schema.name,
        "ordered": schema.ordered,
        "unique": schema.unique,
        "report_duplicates": schema.report_duplicates,
        "unique_column_names": schema.unique_column_names,
        "add_missing_columns": getattr(schema, "add_missing_columns", False),
        "title": schema.title,
        "description": schema.description,
        "metadata": getattr(schema, "metadata", None) or None,
    }


def schema_from_dict(data: dict) -> Any:
    """Reconstruct a pandera DataFrameSchema from a plain dict.

    Both column-level and dataframe-level ``metadata`` are restored.
    """
    from pandera import DataFrameSchema  # lazy import

    data = data or {}

    columns_data = data.get("columns") or {}
    columns = {name: _deserialize_column(col_data) for name, col_data in columns_data.items()}

    index = _deserialize_index(data.get("index"))
    checks = _deserialize_checks(data.get("checks"))

    kwargs: dict[str, Any] = {
        "coerce": data.get("coerce", False),
        "strict": data.get("strict", False),
        "ordered": data.get("ordered", False),
        "report_duplicates": data.get("report_duplicates", "all"),
        "unique_column_names": data.get("unique_column_names", False),
        "add_missing_columns": data.get("add_missing_columns", False),
    }
    if columns:
        kwargs["columns"] = columns
    if checks:
        kwargs["checks"] = checks
    if index is not None:
        kwargs["index"] = index
    for opt in ("name", "unique", "title", "description", "metadata"):
        val = data.get(opt)
        if val is not None:
            kwargs[opt] = val

    return DataFrameSchema(**kwargs)


# ---------------------------------------------------------------------------
# Public YAML / JSON round-trip API
# ---------------------------------------------------------------------------

def to_yaml(dataframe_schema: Any, stream=None) -> str | None:
    """Serialize a DataFrameSchema to YAML.

    Args:
        dataframe_schema: Pandera DataFrameSchema instance.
        stream: Optional file path (str/Path) or writable stream.  When
            ``None``, the YAML string is returned.

    Returns:
        YAML string when *stream* is ``None``, otherwise ``None``.
    """
    data = schema_to_dict(dataframe_schema)

    def _dump(obj: Any, s: Any) -> str | None:
        return yaml.safe_dump(obj, stream=s, sort_keys=False, allow_unicode=True)

    if stream is None:
        return _dump(data, None)

    try:
        with Path(stream).open("w", encoding="utf-8") as f:
            _dump(data, f)
    except (TypeError, OSError):
        _dump(data, stream)
    return None


def from_yaml(yaml_schema: Any) -> Any:
    """Load a DataFrameSchema from a YAML file path, file-like object, or YAML string.

    Args:
        yaml_schema: Path to a YAML file, a file-like object, or a YAML string.

    Returns:
        Reconstructed DataFrameSchema.
    """
    try:
        with Path(yaml_schema).open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except (TypeError, OSError):
        data = yaml.safe_load(yaml_schema)
    return schema_from_dict(data)


def to_json(dataframe_schema: Any, target=None, **kwargs: Any) -> str | None:
    """Serialize a DataFrameSchema to JSON.

    Args:
        dataframe_schema: Pandera DataFrameSchema instance.
        target: Optional file path (str/Path) or writable stream.  When
            ``None``, the JSON string is returned.
        **kwargs: Extra keyword arguments forwarded to :func:`json.dumps`.

    Returns:
        JSON string when *target* is ``None``, otherwise ``None``.
    """
    data = schema_to_dict(dataframe_schema)

    if target is None:
        return json.dumps(data, sort_keys=False, **kwargs)

    if isinstance(target, (str, Path)):
        with Path(target).open("w", encoding="utf-8") as f:
            json.dump(data, fp=f, sort_keys=False, **kwargs)
    else:
        json.dump(data, fp=target, sort_keys=False, **kwargs)
    return None


def from_json(source: Any) -> Any:
    """Load a DataFrameSchema from a JSON file path, file-like object, or JSON string.

    Args:
        source: Path to a JSON file, a file-like object, or a JSON string.

    Returns:
        Reconstructed DataFrameSchema.
    """
    if isinstance(source, str):
        try:
            data = json.loads(source)
        except json.JSONDecodeError:
            with Path(source).open(encoding="utf-8") as f:
                data = json.load(f)
    elif isinstance(source, Path):
        with source.open(encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = json.load(source)
    return schema_from_dict(data)
