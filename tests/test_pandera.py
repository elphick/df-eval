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


def test_df_eval_operations_from_pandera_extracts_kinds():
    """df_eval_operations_from_pandera should detect expr, lookup, and function kinds."""
    schema = pa.DataFrameSchema(
        {
            "value": pa.Column(
                float,
                metadata={"df-eval": {"expr": "2 * base"}},
            ),
            "price": pa.Column(
                float,
                metadata={
                    "df-eval": {
                        "lookup": {
                            "resolver": "prices",
                            "key": "product",
                            "on_missing": "null",
                        }
                    }
                },
            ),
            "score": pa.Column(
                float,
                metadata={
                    "df-eval": {
                        "function": {
                            "name": "dummy_fn",
                            "inputs": ["a"],
                            "outputs": ["score"],
                        }
                    }
                },
            ),
        }
    )

    from df_eval.pandera import df_eval_operations_from_pandera

    ops = df_eval_operations_from_pandera(schema)

    assert ops["value"]["kind"] == "expr"
    assert ops["value"]["expr"] == "2 * base"
    assert ops["price"]["kind"] == "lookup"
    assert ops["price"]["lookup"]["resolver"] == "prices"
    assert ops["score"]["kind"] == "function"
    assert ops["score"]["function"]["name"] == "dummy_fn"


def test_engine_pipeline_function_roundtrip():
    """Engine.register_pipeline_function can be used by metadata-driven ops."""
    schema = pa.DataFrameSchema(
        {
            "a": pa.Column(int),
            "b": pa.Column(int),
            "sum_via_fn": pa.Column(
                int,
                metadata={
                    "df-eval": {
                        "function": {
                            "name": "add_columns",
                            "inputs": ["a", "b"],
                            "outputs": ["sum_via_fn"],
                        }
                    }
                },
            ),
        }
    )

    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    # Simple pipeline function that adds two columns
    def add_columns(df_slice: pd.DataFrame) -> pd.Series:
        return df_slice["a"] + df_slice["b"]

    from df_eval.pandera import df_eval_operations_from_pandera

    engine = Engine()
    engine.register_pipeline_function("add_columns", add_columns)

    ops = df_eval_operations_from_pandera(schema)

    # Manually drive the operation using the private helper for now
    spec = ops["sum_via_fn"]["function"]
    result = engine._apply_pipeline_function(df, spec)

    assert list(result["sum_via_fn"]) == [4, 6]


def test_apply_pandera_schema_with_lookup_and_function_metadata():
    """apply_pandera_schema should honor lookup and function operations end-to-end."""
    from df_eval.lookup import DictResolver

    schema = pa.DataFrameSchema(
        {
            "product": pa.Column(str),
            "quantity": pa.Column(int),
            "price": pa.Column(
                float,
                metadata={
                    "df-eval": {
                        "lookup": {
                            "resolver": "prices",
                            "key": "product",
                            "on_missing": "null",
                        }
                    }
                },
            ),
            "line_total": pa.Column(
                float,
                metadata={"df-eval": {"expr": "price * quantity"}},
            ),
            "discounted_total": pa.Column(
                float,
                metadata={
                    "df-eval": {
                        "function": {
                            "name": "apply_discount",
                            "inputs": ["line_total"],
                            "outputs": ["discounted_total"],
                            "params": {"rate": 0.1},
                        }
                    }
                },
            ),
        }
    )

    df = pd.DataFrame(
        {
            "product": ["apple", "banana", "orange"],
            "quantity": [10, 20, 15],
        }
    )

    price_resolver = DictResolver(
        {
            "apple": 1.50,
            "banana": 0.75,
            "orange": 1.25,
        }
    )

    def apply_discount(df_slice: pd.DataFrame, *, rate: float) -> pd.Series:
        return df_slice["line_total"] * (1 - rate)

    engine = Engine()
    engine.register_resolver("prices", price_resolver)
    engine.register_pipeline_function("apply_discount", apply_discount)

    result = apply_pandera_schema(
        df,
        schema,
        engine=engine,
        coerce=True,
        validate=True,
        validate_post=True,
    )

    assert list(result["price"]) == [1.5, 0.75, 1.25]
    assert list(result["line_total"]) == [15.0, 15.0, 18.75]
    assert list(result["discounted_total"]) == [13.5, 13.5, 16.875]


def test_pandera_schema_yaml_roundtrip_preserves_metadata():
    """YAML schema IO should preserve column metadata, including df-eval keys."""
    from df_eval.pandera import (
        load_pandera_schema_yaml,
        dump_pandera_schema_yaml,
    )

    schema = pa.DataFrameSchema(
        {
            "value": pa.Column(float, metadata={"unit": "kg"}),
            "double": pa.Column(
                float,
                metadata={"df-eval": {"expr": "2 * value"}, "unit": "kg"},
            ),
        }
    )

    yaml_text = dump_pandera_schema_yaml(schema)
    loaded = load_pandera_schema_yaml(yaml_text)

    # Generic metadata preserved
    assert loaded.columns["value"].metadata == {"unit": "kg"}

    # df-eval-specific metadata preserved and usable by our helpers
    expr_map = df_eval_schema_from_pandera(loaded)
    assert expr_map == {"double": "2 * value"}


def test_pandera_schema_json_roundtrip_preserves_metadata():
    """JSON schema IO should preserve column metadata, including df-eval keys."""
    from df_eval.pandera import (
        load_pandera_schema_json,
        dump_pandera_schema_json,
    )

    schema = pa.DataFrameSchema(
        {
            "value": pa.Column(float, metadata={"unit": "kg"}),
            "double": pa.Column(
                float,
                metadata={"df-eval": {"expr": "2 * value"}, "unit": "kg"},
            ),
        }
    )

    json_text = dump_pandera_schema_json(schema)
    loaded = load_pandera_schema_json(json_text)

    assert loaded.columns["value"].metadata == {"unit": "kg"}
    expr_map = df_eval_schema_from_pandera(loaded)
    assert expr_map == {"double": "2 * value"}
