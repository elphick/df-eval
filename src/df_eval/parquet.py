from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any

import pandas as pd


def _import_pyarrow_dataset() -> Any:
    """Import pyarrow.dataset lazily to keep the dependency optional."""
    try:
        import pyarrow.dataset as ds
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ImportError(
            "Parquet helpers require the optional dependency. "
            "Install with: pip install 'df-eval[parquet]'"
        ) from exc
    return ds


def _import_pyarrow_parquet() -> Any:
    """Import pyarrow.parquet lazily to keep the dependency optional."""
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ImportError(
            "Parquet helpers require the optional dependency. "
            "Install with: pip install 'df-eval[parquet]'"
        ) from exc
    return pq


def _import_pyarrow() -> Any:
    """Import pyarrow lazily for table conversion support."""
    try:
        import pyarrow as pa
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ImportError(
            "Parquet helpers require the optional dependency. "
            "Install with: pip install 'df-eval[parquet]'"
        ) from exc
    return pa


def _normalize_columns(columns: Sequence[str] | None) -> list[str] | None:
    """Validate and normalize an optional list of projected column names."""
    if columns is None:
        return None
    if isinstance(columns, (str, bytes)):
        raise TypeError("columns must be a sequence of strings, not a string")

    normalized = list(columns)
    if not all(isinstance(name, str) for name in normalized):
        raise TypeError("columns must only contain strings")

    return normalized


def iter_parquet_row_chunks(
    path: str | Path,
    *,
    chunk_size: int = 100_000,
    columns: Sequence[str] | None = None,
) -> Iterator[pd.DataFrame]:
    """Yield Parquet rows as pandas DataFrame chunks.

    This treats a Parquet file or directory-backed Parquet dataset as an
    out-of-memory DataFrame and streams it into manageable in-memory chunks.

    Args:
        path: Path to a Parquet file or a directory containing a Parquet dataset.
        chunk_size: Maximum number of rows to include per yielded chunk.
        columns: Optional subset of columns to project while scanning.

    Yields:
        DataFrame chunks with at most ``chunk_size`` rows.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
        TypeError: If ``path``, ``chunk_size``, or ``columns`` have invalid types.
        ValueError: If ``chunk_size`` is less than 1.
        ImportError: If ``pyarrow`` is not installed.
    """
    if not isinstance(path, (str, Path)):
        raise TypeError("path must be a str or pathlib.Path")

    parquet_path = Path(path).expanduser()
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet path does not exist: {parquet_path}")

    if isinstance(chunk_size, bool) or not isinstance(chunk_size, int):
        raise TypeError("chunk_size must be an integer")
    if chunk_size < 1:
        raise ValueError("chunk_size must be >= 1")

    projected_columns = _normalize_columns(columns)

    ds = _import_pyarrow_dataset()
    dataset = ds.dataset(parquet_path, format="parquet")

    for batch in dataset.to_batches(columns=projected_columns, batch_size=chunk_size):
        yield batch.to_pandas()


def write_parquet_row_chunks(
    chunks: Iterable[pd.DataFrame],
    output_path: str | Path,
    *,
    compression: str = "snappy",
) -> Path:
    """Write DataFrame chunks to a Parquet file.

    Args:
        chunks: DataFrame chunks to write sequentially.
        output_path: Destination Parquet file path.
        compression: Parquet compression codec.

    Returns:
        The normalized output path.

    Raises:
        TypeError: If ``output_path``, ``compression``, or chunk values are invalid.
        ValueError: If ``compression`` is empty or no chunks are provided.
        ImportError: If ``pyarrow`` is not installed.
    """
    if not isinstance(output_path, (str, Path)):
        raise TypeError("output_path must be a str or pathlib.Path")
    if not isinstance(compression, str):
        raise TypeError("compression must be a string")
    if not compression:
        raise ValueError("compression must not be empty")

    parquet_output_path = Path(output_path).expanduser()
    parquet_output_path.parent.mkdir(parents=True, exist_ok=True)

    pq = _import_pyarrow_parquet()
    pa = _import_pyarrow()

    writer = None
    wrote_any = False
    try:
        for chunk in chunks:
            if not isinstance(chunk, pd.DataFrame):
                raise TypeError("chunks must contain pandas DataFrame values")

            table = pa.Table.from_pandas(chunk)
            if writer is None:
                writer = pq.ParquetWriter(
                    str(parquet_output_path),
                    table.schema,
                    compression=compression,
                )
            writer.write_table(table)
            wrote_any = True
    finally:
        if writer is not None:
            writer.close()

    if not wrote_any:
        raise ValueError("chunks did not yield any DataFrame values")

    return parquet_output_path


__all__ = ["iter_parquet_row_chunks", "write_parquet_row_chunks"]
