"""Tests for Parquet chunk iteration helpers."""

import pandas as pd
import pytest

from df_eval.parquet import iter_parquet_row_chunks

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


def _write_parquet(path, df: pd.DataFrame) -> None:
    """Write a DataFrame as Parquet with pyarrow for deterministic tests."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path)


def test_iter_parquet_row_chunks_single_file_respects_chunk_size(tmp_path):
    """Chunk a single Parquet file into predictable row windows."""
    df = pd.DataFrame({"id": list(range(10)), "value": [x * 2 for x in range(10)]})
    parquet_file = tmp_path / "data.parquet"
    _write_parquet(parquet_file, df)

    chunks = list(iter_parquet_row_chunks(parquet_file, chunk_size=4))

    assert [len(chunk) for chunk in chunks] == [4, 4, 2]
    pd.testing.assert_frame_equal(pd.concat(chunks, ignore_index=True), df)


def test_iter_parquet_row_chunks_projected_columns(tmp_path):
    """Allow column projection to reduce in-memory payload per chunk."""
    df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30], "flag": [True, False, True]})
    parquet_file = tmp_path / "projected.parquet"
    _write_parquet(parquet_file, df)

    chunks = list(iter_parquet_row_chunks(parquet_file, chunk_size=2, columns=["value", "flag"]))

    assert all(list(chunk.columns) == ["value", "flag"] for chunk in chunks)
    pd.testing.assert_frame_equal(
        pd.concat(chunks, ignore_index=True),
        df[["value", "flag"]],
    )


def test_iter_parquet_row_chunks_directory_dataset(tmp_path):
    """Treat a directory of Parquet files as one out-of-memory DataFrame."""
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()

    part_a = pd.DataFrame({"id": [1, 2, 3], "source": ["a", "a", "a"]})
    part_b = pd.DataFrame({"id": [4, 5], "source": ["b", "b"]})
    _write_parquet(dataset_dir / "part-001.parquet", part_a)
    _write_parquet(dataset_dir / "part-002.parquet", part_b)

    chunks = list(iter_parquet_row_chunks(dataset_dir, chunk_size=2))
    combined = pd.concat(chunks, ignore_index=True).sort_values("id").reset_index(drop=True)
    expected = pd.concat([part_a, part_b], ignore_index=True).sort_values("id").reset_index(drop=True)

    assert all(len(chunk) <= 2 for chunk in chunks)
    pd.testing.assert_frame_equal(combined, expected)


@pytest.mark.parametrize(
    ("kwargs", "error_type", "match"),
    [
        ({"chunk_size": 0}, ValueError, "chunk_size must be >= 1"),
        ({"chunk_size": True}, TypeError, "chunk_size must be an integer"),
        ({"columns": "value"}, TypeError, "columns must be a sequence of strings"),
        ({"columns": ["value", 1]}, TypeError, "columns must only contain strings"),
    ],
)
def test_iter_parquet_row_chunks_rejects_invalid_arguments(tmp_path, kwargs, error_type, match):
    """Validate argument shape before scanning the dataset."""
    df = pd.DataFrame({"value": [1, 2, 3]})
    parquet_file = tmp_path / "invalid-args.parquet"
    _write_parquet(parquet_file, df)

    with pytest.raises(error_type, match=match):
        list(iter_parquet_row_chunks(parquet_file, **kwargs))


def test_iter_parquet_row_chunks_raises_for_missing_path(tmp_path):
    """Fail fast with a clear path error when data is missing."""
    missing_path = tmp_path / "missing.parquet"

    with pytest.raises(FileNotFoundError, match="Parquet path does not exist"):
        list(iter_parquet_row_chunks(missing_path))

