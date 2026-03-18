"""Tests for Engine Parquet streaming workflows."""

from pathlib import Path

import pandas as pd
import pytest

from df_eval.engine import Engine
from df_eval.parquet import iter_parquet_row_chunks

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    """Write DataFrame input for Parquet integration tests."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path)


def test_engine_apply_schema_parquet_to_parquet(tmp_path):
    """Process parquet-in/parquet-out without loading all rows at once."""
    source = pd.DataFrame({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    input_path = tmp_path / "input.parquet"
    output_path = tmp_path / "output.parquet"
    _write_parquet(input_path, source)

    schema = {
        "sum": "a + b",
        "double_sum": "sum * 2",
    }

    engine = Engine()
    returned_path = engine.apply_schema_parquet_to_parquet(
        input_path,
        output_path,
        schema,
        chunk_size=2,
    )

    assert returned_path == output_path

    chunks = list(iter_parquet_row_chunks(output_path, chunk_size=10))
    result = pd.concat(chunks, ignore_index=True)
    expected = engine.apply_schema(source, schema)
    pd.testing.assert_frame_equal(result, expected)


def test_engine_iter_apply_schema_parquet_chunks_supports_projection(tmp_path):
    """Allow efficient input projection while applying derived columns."""
    source = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [10, 20, 30],
            "unused": [100, 200, 300],
        }
    )
    input_path = tmp_path / "projection-input.parquet"
    _write_parquet(input_path, source)

    engine = Engine()
    chunks = list(
        engine.iter_apply_schema_parquet_chunks(
            input_path,
            {"sum": "a + b"},
            chunk_size=2,
            input_columns=["a", "b"],
        )
    )

    assert all(len(chunk) <= 2 for chunk in chunks)
    combined = pd.concat(chunks, ignore_index=True)
    assert list(combined.columns) == ["a", "b", "sum"]
    assert list(combined["sum"]) == [11, 22, 33]


def test_engine_apply_schema_parquet_to_df(tmp_path):
    """Return a single in-memory DataFrame after chunked parquet transforms."""
    source = pd.DataFrame({"a": [2, 4, 6], "b": [1, 3, 5]})
    input_path = tmp_path / "to-df-input.parquet"
    _write_parquet(input_path, source)

    schema = {"sum": "a + b", "scaled": "sum * 10"}
    engine = Engine()

    result = engine.apply_schema_parquet_to_df(input_path, schema, chunk_size=2)
    expected = engine.apply_schema(source, schema)

    pd.testing.assert_frame_equal(result, expected)


def test_engine_apply_schema_parquet_to_df_supports_projection(tmp_path):
    """Forward projection settings through to parquet chunk scanning."""
    source = pd.DataFrame(
        {
            "a": [3, 6, 9],
            "b": [2, 4, 8],
            "unused": [100, 200, 300],
        }
    )
    input_path = tmp_path / "to-df-projection.parquet"
    _write_parquet(input_path, source)

    engine = Engine()
    result = engine.apply_schema_parquet_to_df(
        input_path,
        {"sum": "a + b"},
        chunk_size=2,
        input_columns=["a", "b"],
    )

    assert list(result.columns) == ["a", "b", "sum"]
    assert list(result["sum"]) == [5, 10, 17]


def test_engine_apply_schema_parquet_to_parquet_supports_output_columns(tmp_path):
    """Write only selected columns in requested order."""
    source = pd.DataFrame({"a": [1, 2], "b": [3, 4], "extra": [7, 8]})
    input_path = tmp_path / "output-columns-input.parquet"
    output_path = tmp_path / "output-columns-output.parquet"
    _write_parquet(input_path, source)

    engine = Engine()
    engine.apply_schema_parquet_to_parquet(
        input_path,
        output_path,
        {"sum": "a + b"},
        input_columns=["a", "b"],
        output_columns=["sum", "a", "b"],
        chunk_size=1,
    )

    result = pd.concat(list(iter_parquet_row_chunks(output_path, chunk_size=10)), ignore_index=True)
    assert list(result.columns) == ["sum", "a", "b"]
    assert list(result["sum"]) == [4, 6]


