"""Tests for Pandera integration helpers."""

import pandas as pd
import pytest

from df_eval.engine import Engine
from df_eval.pandera import (
    apply_pandera_schema,
    apply_pandera_schema_parquet_to_parquet,
    df_eval_schema_from_pandera,
)
from df_eval.parquet import iter_parquet_row_chunks

pa = pytest.importorskip("pandera")


def _schema_model_base():
    return getattr(pa, "DataFrameModel", getattr(pa, "SchemaModel", None))


def test_df_eval_schema_from_pandera_extracts_expressions():
    """Extract only columns that provide a df-eval expression."""
    schema = pa.DataFrameSchema(
        {
            "value": pa.Column(float),
            "double": pa.Column(
                float,
                metadata={"df-eval": {"expr": "2 * value"}},
            ),
            "ignored": pa.Column(float, metadata={"foo": "bar"}),
        }
    )

    expr_map = df_eval_schema_from_pandera(schema)

    assert expr_map == {"double": "2 * value"}


def test_df_eval_schema_from_pandera_raises_for_non_string_expr():
    """Guard against invalid metadata that cannot be evaluated."""
    schema = pa.DataFrameSchema(
        {
            "value": pa.Column(float),
            "bad": pa.Column(float, metadata={"df-eval": {"expr": 42}}),
        }
    )

    with pytest.raises(TypeError, match="must be a string"):
        df_eval_schema_from_pandera(schema)


def test_df_eval_schema_from_model_class():
    """Accept Pandera model classes and normalize to DataFrameSchema."""
    model_base = _schema_model_base()
    if model_base is None:
        pytest.skip("Pandera model base class unavailable")

    try:
        from pandera.typing import Series
    except ImportError:
        pytest.skip("pandera.typing.Series unavailable")

    class MySchema(model_base):
        value: Series[float] = pa.Field(coerce=True)
        double: Series[float] = pa.Field(
            coerce=True,
            metadata={"df-eval": {"expr": "2 * value"}},
        )

    expr_map = df_eval_schema_from_pandera(MySchema)

    assert expr_map == {"double": "2 * value"}


def test_apply_pandera_schema_validates_then_derives_then_validates_full_schema():
    """Pre-validation excludes derived columns and post-validation enforces full schema."""
    schema = pa.DataFrameSchema(
        {
            "a": pa.Column(int),
            "b": pa.Column(int),
            "sum": pa.Column(
                int,
                metadata={"df-eval": {"expr": "a + b"}},
                checks=pa.Check.ge(0),
            ),
        }
    )
    df = pd.DataFrame({"a": ["1", "2"], "b": ["3", "4"]})

    result = apply_pandera_schema(df, schema, validate=True, coerce=True)

    assert list(result["sum"]) == [4, 6]
    assert str(result["a"].dtype).startswith("int")


def test_apply_pandera_schema_rejects_overwrite_by_default():
    """Prevent accidental silent overwrite when input already has a derived column."""
    schema = pa.DataFrameSchema(
        {
            "a": pa.Column(int),
            "b": pa.Column(int),
            "sum": pa.Column(int, metadata={"df-eval": {"expr": "a + b"}}),
        }
    )
    df = pd.DataFrame({"a": [1], "b": [2], "sum": [999]})

    with pytest.raises(ValueError, match="already contains derived columns"):
        apply_pandera_schema(df, schema)


def test_apply_pandera_schema_can_skip_post_validation():
    """Allow deriving columns without validating derived dtype constraints."""
    schema = pa.DataFrameSchema(
        {
            "a": pa.Column(int),
            "b": pa.Column(int),
            "ratio": pa.Column(int, metadata={"df-eval": {"expr": "a / b"}}),
        }
    )
    df = pd.DataFrame({"a": [1, 2], "b": [2, 2]})

    result = apply_pandera_schema(df, schema, validate_post=False)
    assert list(result["ratio"]) == [0.5, 1.0]

    with pytest.raises(pa.errors.SchemaError):
        apply_pandera_schema(df, schema, validate_post=True, coerce=False)


def test_engine_apply_pandera_schema_matches_functional_helper():
    """Engine façade should delegate to Pandera helper with equivalent behavior."""
    schema = pa.DataFrameSchema(
        {
            "a": pa.Column(int),
            "b": pa.Column(int),
            "sum": pa.Column(int, metadata={"df-eval": {"expr": "a + b + SHIFT"}}),
        }
    )
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    engine = Engine()
    engine.register_constant("SHIFT", 10)

    via_engine = engine.apply_pandera_schema(df, schema)
    via_helper = apply_pandera_schema(df, schema, engine=engine)

    pd.testing.assert_frame_equal(via_engine, via_helper)
    assert list(via_engine["sum"]) == [14, 16]


def test_apply_pandera_schema_parquet_to_parquet_plans_projection_and_order(monkeypatch, tmp_path):
    """Pandera parquet helper should derive minimal inputs and ordered outputs."""
    schema = pa.DataFrameSchema(
        {
            "a": pa.Column(int),
            "b": pa.Column(int),
            "sum": pa.Column(int, metadata={"df-eval": {"expr": "a + b"}}),
        }
    )

    captured: dict[str, object] = {}

    def _capture(*args, **kwargs):
        captured["input_columns"] = kwargs.get("input_columns")
        captured["output_columns"] = kwargs.get("output_columns")
        return tmp_path / "out.parquet"

    engine = Engine()
    monkeypatch.setattr(engine, "apply_schema_parquet_to_parquet", _capture)

    result = apply_pandera_schema_parquet_to_parquet(
        "input.parquet",
        tmp_path / "out.parquet",
        schema,
        engine=engine,
    )

    assert result == tmp_path / "out.parquet"
    assert captured["input_columns"] == ["a", "b"]
    assert captured["output_columns"] == ["a", "b", "sum"]


def test_engine_apply_pandera_schema_parquet_to_parquet_writes_schema_order(tmp_path):
    """Engine façade should write only schema columns in schema order."""
    pa_arrow = pytest.importorskip("pyarrow")
    pq_arrow = pytest.importorskip("pyarrow.parquet")

    schema = pa.DataFrameSchema(
        {
            "a": pa.Column(int),
            "b": pa.Column(int),
            "sum": pa.Column(int, metadata={"df-eval": {"expr": "a + b"}}),
            "scaled": pa.Column(int, metadata={"df-eval": {"expr": "sum * 10"}}),
        }
    )

    input_df = pd.DataFrame({"unused": [9, 9], "b": [3, 4], "a": [1, 2]})
    input_path = tmp_path / "pandera-in.parquet"
    output_path = tmp_path / "pandera-out.parquet"
    pq_arrow.write_table(pa_arrow.Table.from_pandas(input_df, preserve_index=False), input_path)

    engine = Engine()
    returned = engine.apply_pandera_schema_parquet_to_parquet(
        input_path,
        output_path,
        schema,
        chunk_size=1,
    )

    assert returned == output_path
    output_df = pd.concat(list(iter_parquet_row_chunks(output_path, chunk_size=10)), ignore_index=True)
    assert list(output_df.columns) == ["a", "b", "sum", "scaled"]
    assert list(output_df["sum"]) == [4, 6]
    assert list(output_df["scaled"]) == [40, 60]


