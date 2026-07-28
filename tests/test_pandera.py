"""Tests for Pandera integration helpers."""

import pandas as pd
import pytest

from df_eval.engine import Engine
from df_eval.pandera import (
    apply_aliases,
    apply_decimals,
    apply_pandera_schema,
    apply_pandera_schema_parquet_to_parquet,
    df_eval_schema_from_pandera,
    validate_df_eval_schema,
)
from df_eval.pandera import _classify_aliases, _extract_aliases
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


def test_apply_aliases_maps_source_column_to_target():
    """Alias transform should materialize missing target columns from source columns."""
    schema = pa.DataFrameSchema(
        {
            "legacy_price": pa.Column(float),
            "price": pa.Column(float, metadata={"df-eval": {"alias": "legacy_price"}}),
        }
    )
    df = pd.DataFrame({"legacy_price": [10.111, 20.222]})

    result = apply_aliases(df, schema)

    assert list(result["price"]) == [10.111, 20.222]
    assert list(result["legacy_price"]) == [10.111, 20.222]


def test_apply_aliases_rejects_source_target_collision():
    """Alias transform should fail when source and target are both present."""
    schema = pa.DataFrameSchema(
        {
            "legacy_price": pa.Column(float),
            "price": pa.Column(float, metadata={"df-eval": {"alias": "legacy_price"}}),
        }
    )
    df = pd.DataFrame({"legacy_price": [10.0], "price": [9.0]})

    with pytest.raises(ValueError, match="alias target and source columns"):
        apply_aliases(df, schema)


def test_apply_pandera_schema_allows_missing_alias_source_when_target_exists():
    """Alias source should be optional when the target column is already provided."""
    schema = pa.DataFrameSchema(
        {
            "legacy_price": pa.Column(float, coerce=True),
            "price": pa.Column(float, coerce=True, metadata={"df-eval": {"alias": "legacy_price"}}),
            "taxed": pa.Column(float, coerce=True, metadata={"df-eval": {"expr": "price * 1.075"}}),
        }
    )
    df = pd.DataFrame({"price": [10.0, 20.0]})

    result = apply_pandera_schema(df, schema, validate=True, coerce=True, validate_post=True)

    assert list(result["price"]) == [10.0, 20.0]
    assert pytest.approx(list(result["taxed"])) == [10.75, 21.5]
    assert "legacy_price" not in result.columns


def test_apply_decimals_rounds_existing_columns_only():
    """Decimals transform should round already-materialized columns only."""
    schema = pa.DataFrameSchema(
        {
            "price": pa.Column(float, metadata={"df-eval": {"decimals": 1}}),
            "taxed": pa.Column(float, metadata={"df-eval": {"expr": "price * 1.075", "decimals": 2}}),
        }
    )
    df = pd.DataFrame({"price": [10.16, 20.54]})

    result = apply_decimals(df, schema)

    assert list(result["price"]) == [10.2, 20.5]
    assert "taxed" not in result.columns


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


def test_apply_pandera_schema_applies_aliases_and_decimals_before_expr():
    """Alias and decimals transforms should run before expression operations."""
    schema = pa.DataFrameSchema(
        {
            "legacy_price": pa.Column(float, coerce=True),
            "price": pa.Column(
                float,
                coerce=True,
                metadata={"df-eval": {"alias": "legacy_price", "decimals": 1}},
            ),
            "taxed": pa.Column(
                float,
                coerce=True,
                metadata={"df-eval": {"expr": "price * 1.075"}},
            ),
        }
    )
    df = pd.DataFrame({"legacy_price": [10.16, 20.54]})

    result = apply_pandera_schema(df, schema, validate=True, coerce=True, validate_post=True)

    assert list(result["price"]) == [10.2, 20.5]
    assert pytest.approx(list(result["taxed"])) == [10.965, 22.0375]


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


def test_apply_pandera_schema_parquet_to_parquet_plans_projection_and_order(tmp_path):
    """Pandera parquet helper should derive minimal inputs and produce ordered outputs."""
    pa_arrow = pytest.importorskip("pyarrow")
    pq_arrow = pytest.importorskip("pyarrow.parquet")

    schema = pa.DataFrameSchema(
        {
            "a": pa.Column(int),
            "b": pa.Column(int),
            "sum": pa.Column(int, metadata={"df-eval": {"expr": "a + b"}}),
        }
    )

    input_df = pd.DataFrame({"extra": [9, 9], "b": [3, 4], "a": [1, 2]})
    input_path = tmp_path / "in.parquet"
    output_path = tmp_path / "out.parquet"
    pq_arrow.write_table(
        pa_arrow.Table.from_pandas(input_df, preserve_index=False), input_path
    )

    result = apply_pandera_schema_parquet_to_parquet(
        input_path,
        output_path,
        schema,
    )

    assert result == output_path
    output_df = pd.read_parquet(output_path)
    assert list(output_df.columns) == ["a", "b", "sum"]
    assert list(output_df["sum"]) == [4, 6]


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


def test_df_eval_operations_from_pandera_extracts_decimals():
    """Operations extraction should preserve optional decimals metadata."""
    schema = pa.DataFrameSchema(
        {
            "base": pa.Column(float),
            "rounded": pa.Column(
                float,
                metadata={"df-eval": {"expr": "base / 3", "decimals": 2}},
            ),
        }
    )

    from df_eval.pandera import df_eval_operations_from_pandera

    ops = df_eval_operations_from_pandera(schema)
    assert ops["rounded"]["decimals"] == 2


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


def test_apply_pandera_schema_applies_decimals_rounding():
    """Pandera metadata decimals should round derived outputs in the pipeline."""
    schema = pa.DataFrameSchema(
        {
            "price": pa.Column(float, coerce=True),
            "taxed": pa.Column(
                float,
                coerce=True,
                metadata={"df-eval": {"expr": "price * 1.075", "decimals": 2}},
            ),
        }
    )
    df = pd.DataFrame({"price": [10.111, 20.555]})

    result = apply_pandera_schema(df, schema, validate=True, coerce=True, validate_post=True)

    assert list(result["taxed"]) == [10.87, 22.1]


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


def test_pandera_schema_yaml_roundtrip_preserves_dataframe_metadata():
    """YAML schema IO should preserve metadata at the dataframe (schema) level."""
    from df_eval.pandera import (
        load_pandera_schema_yaml,
        dump_pandera_schema_yaml,
    )

    schema = pa.DataFrameSchema(
        {
            "value": pa.Column(float, metadata={"unit": "kg"}),
            "double": pa.Column(
                float,
                metadata={"df-eval": {"expr": "2 * value"}},
            ),
        },
        metadata={"source": "sensor-1", "owner": "team-a"},
        title="SensorReadings",
        description="Sensor reading schema",
    )

    yaml_text = dump_pandera_schema_yaml(schema)
    loaded = load_pandera_schema_yaml(yaml_text)

    assert loaded.metadata == {"source": "sensor-1", "owner": "team-a"}
    assert loaded.title == "SensorReadings"
    assert loaded.description == "Sensor reading schema"
    # Column metadata still preserved alongside schema metadata
    assert loaded.columns["value"].metadata == {"unit": "kg"}
    expr_map = df_eval_schema_from_pandera(loaded)
    assert expr_map == {"double": "2 * value"}


def test_pandera_schema_json_roundtrip_preserves_dataframe_metadata():
    """JSON schema IO should preserve metadata at the dataframe (schema) level."""
    from df_eval.pandera import (
        load_pandera_schema_json,
        dump_pandera_schema_json,
    )

    schema = pa.DataFrameSchema(
        {
            "value": pa.Column(float, metadata={"unit": "kg"}),
            "double": pa.Column(
                float,
                metadata={"df-eval": {"expr": "2 * value"}},
            ),
        },
        metadata={"source": "sensor-1", "owner": "team-a"},
        title="SensorReadings",
        description="Sensor reading schema",
    )

    json_text = dump_pandera_schema_json(schema)
    loaded = load_pandera_schema_json(json_text)

    assert loaded.metadata == {"source": "sensor-1", "owner": "team-a"}
    assert loaded.title == "SensorReadings"
    assert loaded.description == "Sensor reading schema"
    assert loaded.columns["value"].metadata == {"unit": "kg"}
    expr_map = df_eval_schema_from_pandera(loaded)
    assert expr_map == {"double": "2 * value"}


def test_pandera_schema_yaml_roundtrip_preserves_checks():
    """YAML schema IO should preserve column checks through a full round-trip."""
    from df_eval.pandera import (
        load_pandera_schema_yaml,
        dump_pandera_schema_yaml,
    )

    schema = pa.DataFrameSchema(
        {
            "score": pa.Column(
                float,
                checks=[pa.Check.ge(0), pa.Check.le(100)],
                metadata={"df-eval": {"expr": "base * 10"}},
            ),
        }
    )

    yaml_text = dump_pandera_schema_yaml(schema)
    loaded = load_pandera_schema_yaml(yaml_text)

    check_names = {c.name for c in loaded.columns["score"].checks}
    assert "greater_than_or_equal_to" in check_names
    assert "less_than_or_equal_to" in check_names
    assert loaded.columns["score"].metadata == {"df-eval": {"expr": "base * 10"}}


def test_pandera_schema_yaml_roundtrip_file(tmp_path):
    """YAML schema IO should correctly write to and read from a file path."""
    from df_eval.pandera import (
        load_pandera_schema_yaml,
        dump_pandera_schema_yaml,
    )

    schema = pa.DataFrameSchema(
        {
            "value": pa.Column(float, metadata={"unit": "kg"}),
            "double": pa.Column(
                float,
                metadata={"df-eval": {"expr": "2 * value"}},
            ),
        },
        metadata={"source": "file-test"},
    )

    yaml_path = tmp_path / "schema.yaml"
    result = dump_pandera_schema_yaml(schema, stream=yaml_path)
    assert result is None  # writing to file returns None
    assert yaml_path.exists()

    loaded = load_pandera_schema_yaml(yaml_path)
    assert loaded.metadata == {"source": "file-test"}
    assert loaded.columns["value"].metadata == {"unit": "kg"}
    expr_map = df_eval_schema_from_pandera(loaded)
    assert expr_map == {"double": "2 * value"}


def test_pandera_schema_json_roundtrip_file(tmp_path):
    """JSON schema IO should correctly write to and read from a file path."""
    from df_eval.pandera import (
        load_pandera_schema_json,
        dump_pandera_schema_json,
    )

    schema = pa.DataFrameSchema(
        {
            "value": pa.Column(float, metadata={"unit": "kg"}),
            "double": pa.Column(
                float,
                metadata={"df-eval": {"expr": "2 * value"}},
            ),
        },
        metadata={"source": "file-test"},
    )

    json_path = tmp_path / "schema.json"
    result = dump_pandera_schema_json(schema, target=json_path)
    assert result is None  # writing to file returns None
    assert json_path.exists()

    loaded = load_pandera_schema_json(json_path)
    assert loaded.metadata == {"source": "file-test"}
    assert loaded.columns["value"].metadata == {"unit": "kg"}
    expr_map = df_eval_schema_from_pandera(loaded)
    assert expr_map == {"double": "2 * value"}


def test_apply_aliases_skips_silently_when_neither_source_nor_target_exists():
    """Alias transform should skip silently when both source and target are missing.
    
    This is the correct behavior for optional aliases that may not apply to all DataFrames.
    """
    schema = pa.DataFrameSchema(
        {
            "legacy_price": pa.Column(float),
            "price": pa.Column(float, metadata={"df-eval": {"alias": "legacy_price"}}),
            "other_col": pa.Column(int),
        }
    )
    df = pd.DataFrame({"other_col": [1, 2]})

    # Should not raise, should skip silently
    result = apply_aliases(df, schema)

    assert list(result["other_col"]) == [1, 2]
    assert "price" not in result.columns
    assert "legacy_price" not in result.columns


def test_apply_pandera_schema_with_strict_filter_and_native_alias_target():
    """Regression test for alias bug: strict filter should preserve native alias targets.
    
    When an alias target (e.g., 'price') is the native column name in the DataFrame,
    and an alias source (e.g., 'legacy_price') is not present, the schema validation
    with strict='filter' should not drop the native column before apply_aliases runs.
    
    This test reproduces the exact scenario from the bug report.
    """
    schema = pa.DataFrameSchema(
        {
            "legacy_price": pa.Column(float, coerce=True),
            "price": pa.Column(float, coerce=True, metadata={"df-eval": {"alias": "legacy_price"}}),
            "taxed": pa.Column(float, coerce=True, metadata={"df-eval": {"expr": "price * 1.075"}}),
        },
        strict="filter"
    )
    df = pd.DataFrame({"price": [10.0, 20.0]})

    result = apply_pandera_schema(df, schema, validate=True, coerce=True, validate_post=True)

    assert list(result["price"]) == [10.0, 20.0]
    assert pytest.approx(list(result["taxed"])) == [10.75, 21.5]
    assert "legacy_price" not in result.columns


def test_apply_pandera_schema_strict_filter_with_alias_source_not_target():
    """Verify alias still works with strict filter when source column is provided."""
    schema = pa.DataFrameSchema(
        {
            "legacy_price": pa.Column(float, coerce=True),
            "price": pa.Column(float, coerce=True, metadata={"df-eval": {"alias": "legacy_price"}}),
            "taxed": pa.Column(float, coerce=True, metadata={"df-eval": {"expr": "price * 1.075"}}),
        },
        strict="filter"
    )
    df = pd.DataFrame({"legacy_price": [10.0, 20.0]})

    result = apply_pandera_schema(df, schema, validate=True, coerce=True, validate_post=True)

    assert list(result["price"]) == [10.0, 20.0]
    assert list(result["legacy_price"]) == [10.0, 20.0]
    assert pytest.approx(list(result["taxed"])) == [10.75, 21.5]


def test_apply_pandera_schema_strict_filter_with_multiple_aliases():
    """Test strict filter with multiple aliases to ensure all are handled correctly."""
    schema = pa.DataFrameSchema(
        {
            "centroid_x": pa.Column(float, coerce=True),
            "centroid_y": pa.Column(float, coerce=True),
            "x": pa.Column(float, coerce=True, metadata={"df-eval": {"alias": "centroid_x"}}),
            "y": pa.Column(float, coerce=True, metadata={"df-eval": {"alias": "centroid_y"}}),
            "distance": pa.Column(float, coerce=True, metadata={"df-eval": {"expr": "(x**2 + y**2)**0.5"}}),
        },
        strict="filter"
    )
    df = pd.DataFrame({"x": [3.0, 4.0], "y": [4.0, 3.0]})

    result = apply_pandera_schema(df, schema, validate=True, coerce=True, validate_post=True)

    assert list(result["x"]) == [3.0, 4.0]
    assert list(result["y"]) == [4.0, 3.0]
    assert pytest.approx(list(result["distance"])) == [5.0, 5.0]
    assert "centroid_x" not in result.columns
    assert "centroid_y" not in result.columns


def test_apply_aliases_still_rejects_collision_after_fix():
    """Verify that the fix still correctly rejects when both source and target exist."""
    schema = pa.DataFrameSchema(
        {
            "legacy_price": pa.Column(float),
            "price": pa.Column(float, metadata={"df-eval": {"alias": "legacy_price"}}),
        }
    )
    df = pd.DataFrame({"legacy_price": [10.0], "price": [9.0]})

    with pytest.raises(ValueError, match="alias target and source columns"):
        apply_aliases(df, schema)


# ---------------------------------------------------------------------------
# _classify_aliases unit tests
# ---------------------------------------------------------------------------


def test_classify_aliases_rename():
    """Single present source, absent target → RENAME."""
    classified = _classify_aliases({"price": ["legacy_price"]}, {"legacy_price", "qty"})
    assert classified["price"] == ("RENAME", "legacy_price")


def test_classify_aliases_native():
    """Target present, no source present → NATIVE."""
    classified = _classify_aliases({"price": ["legacy_price"]}, {"price", "qty"})
    assert classified["price"] == ("NATIVE", "")


def test_classify_aliases_ambiguous_both_present():
    """Target and source both present → AMBIGUOUS."""
    classified = _classify_aliases(
        {"price": ["legacy_price"]}, {"price", "legacy_price"}
    )
    assert classified["price"][0] == "AMBIGUOUS"
    assert classified["price"][1] == "legacy_price"


def test_classify_aliases_ambiguous_multiple_sources():
    """Multiple sources present → AMBIGUOUS."""
    classified = _classify_aliases(
        {"price": ["legacy_price", "old_price"]},
        {"legacy_price", "old_price"},
    )
    assert classified["price"][0] == "AMBIGUOUS"


def test_classify_aliases_absent():
    """Neither target nor any source present → ABSENT."""
    classified = _classify_aliases({"price": ["legacy_price"]}, {"qty"})
    assert classified["price"] == ("ABSENT", "")


def test_classify_aliases_multi_source_single_match():
    """First matching source among multiple candidates → RENAME."""
    classified = _classify_aliases(
        {"deposit_code": ["deposit", "dep"]},
        {"dep", "qty"},
    )
    assert classified["deposit_code"] == ("RENAME", "dep")


def test_classify_aliases_empty():
    """Empty aliases dict returns empty result."""
    assert _classify_aliases({}, {"a", "b"}) == {}


# ---------------------------------------------------------------------------
# _extract_aliases — list alias support
# ---------------------------------------------------------------------------


def test_extract_aliases_accepts_string():
    """Single-string alias is converted to a single-element list internally."""
    from df_eval.pandera import _extract_aliases, _to_dataframe_schema, _import_pandera

    schema = pa.DataFrameSchema(
        {
            "legacy_price": pa.Column(float),
            "price": pa.Column(
                float, metadata={"df-eval": {"alias": "legacy_price"}}
            ),
        }
    )
    pa_inner = _import_pandera()
    df_sch = _to_dataframe_schema(schema, pa_inner)
    aliases = _extract_aliases(df_sch, meta_key="df-eval")
    assert aliases == {"price": ["legacy_price"]}


def test_extract_aliases_accepts_list():
    """List alias is accepted and preserved."""
    from df_eval.pandera import _extract_aliases, _to_dataframe_schema, _import_pandera

    schema = pa.DataFrameSchema(
        {
            "deposit": pa.Column(float),
            "dep": pa.Column(float),
            "deposit_code": pa.Column(
                float,
                metadata={"df-eval": {"alias": ["deposit", "dep"]}},
            ),
        }
    )
    pa_inner = _import_pandera()
    df_sch = _to_dataframe_schema(schema, pa_inner)
    aliases = _extract_aliases(df_sch, meta_key="df-eval")
    assert aliases == {"deposit_code": ["deposit", "dep"]}


def test_extract_aliases_rejects_empty_list():
    """Empty list alias raises ValueError."""
    from df_eval.pandera import _extract_aliases, _to_dataframe_schema, _import_pandera

    schema = pa.DataFrameSchema(
        {
            "price": pa.Column(float, metadata={"df-eval": {"alias": []}}),
        }
    )
    pa_inner = _import_pandera()
    df_sch = _to_dataframe_schema(schema, pa_inner)
    with pytest.raises(ValueError, match="must not be empty"):
        _extract_aliases(df_sch, meta_key="df-eval")


def test_extract_aliases_rejects_non_string_in_list():
    """List alias with non-string element raises TypeError."""
    from df_eval.pandera import _extract_aliases, _to_dataframe_schema, _import_pandera

    schema = pa.DataFrameSchema(
        {
            "legacy": pa.Column(float),
            "price": pa.Column(float, metadata={"df-eval": {"alias": ["legacy", 42]}}),
        }
    )
    pa_inner = _import_pandera()
    df_sch = _to_dataframe_schema(schema, pa_inner)
    with pytest.raises(TypeError, match="list must contain only strings"):
        _extract_aliases(df_sch, meta_key="df-eval")


# ---------------------------------------------------------------------------
# apply_aliases — multi-source alias (list)
# ---------------------------------------------------------------------------


def test_apply_aliases_multi_source_first_match():
    """Multi-source alias uses the first present candidate."""
    schema = pa.DataFrameSchema(
        {
            "deposit": pa.Column(float),
            "dep": pa.Column(float),
            "deposit_code": pa.Column(
                float,
                metadata={"df-eval": {"alias": ["deposit", "dep"]}},
            ),
        }
    )
    df = pd.DataFrame({"dep": [1.0, 2.0]})
    result = apply_aliases(df, schema)
    assert list(result["deposit_code"]) == [1.0, 2.0]
    assert list(result["dep"]) == [1.0, 2.0]  # source preserved


def test_apply_aliases_multi_source_absent_skips_silently():
    """Multi-source alias with no candidates skips silently."""
    schema = pa.DataFrameSchema(
        {
            "deposit": pa.Column(float),
            "dep": pa.Column(float),
            "deposit_code": pa.Column(
                float,
                metadata={"df-eval": {"alias": ["deposit", "dep"]}},
            ),
        }
    )
    df = pd.DataFrame({"other": [1.0]})
    result = apply_aliases(df, schema)
    assert "deposit_code" not in result.columns


def test_apply_aliases_multi_source_ambiguous_raises():
    """Multi-source alias raises when more than one candidate is present."""
    schema = pa.DataFrameSchema(
        {
            "deposit": pa.Column(float),
            "dep": pa.Column(float),
            "deposit_code": pa.Column(
                float,
                metadata={"df-eval": {"alias": ["deposit", "dep"]}},
            ),
        }
    )
    df = pd.DataFrame({"deposit": [1.0], "dep": [2.0]})
    with pytest.raises(ValueError, match="alias target and source columns"):
        apply_aliases(df, schema)


# ---------------------------------------------------------------------------
# apply_pandera_schema — multi-source alias end-to-end
# ---------------------------------------------------------------------------


def test_apply_pandera_schema_multi_source_alias_rename():
    """Multi-source alias renames the matching candidate before validation."""
    schema = pa.DataFrameSchema(
        {
            "deposit": pa.Column(float, coerce=True),
            "dep": pa.Column(float, coerce=True),
            "deposit_code": pa.Column(
                float, coerce=True, metadata={"df-eval": {"alias": ["deposit", "dep"]}}
            ),
            "doubled": pa.Column(
                float, coerce=True, metadata={"df-eval": {"expr": "deposit_code * 2"}}
            ),
        },
        strict="filter",
    )
    df = pd.DataFrame({"dep": [5.0, 10.0]})
    result = apply_pandera_schema(df, schema, validate=True, coerce=True, validate_post=True)
    assert list(result["deposit_code"]) == [5.0, 10.0]
    assert list(result["doubled"]) == [10.0, 20.0]


def test_apply_pandera_schema_multi_source_alias_native():
    """Multi-source alias is a no-op when target is already present."""
    schema = pa.DataFrameSchema(
        {
            "deposit": pa.Column(float, coerce=True),
            "dep": pa.Column(float, coerce=True),
            "deposit_code": pa.Column(
                float, coerce=True, metadata={"df-eval": {"alias": ["deposit", "dep"]}}
            ),
        },
        strict="filter",
    )
    df = pd.DataFrame({"deposit_code": [7.0]})
    result = apply_pandera_schema(df, schema, validate=True, coerce=True, validate_post=True)
    assert list(result["deposit_code"]) == [7.0]


def test_apply_pandera_schema_ambiguous_raises():
    """AMBIGUOUS alias raises ValueError before any validation occurs."""
    schema = pa.DataFrameSchema(
        {
            "legacy_price": pa.Column(float, coerce=True),
            "price": pa.Column(
                float, coerce=True, metadata={"df-eval": {"alias": "legacy_price"}}
            ),
        }
    )
    df = pd.DataFrame({"legacy_price": [10.0], "price": [9.0]})
    with pytest.raises(ValueError, match="alias target and source columns"):
        apply_pandera_schema(df, schema)


# ---------------------------------------------------------------------------
# validate_df_eval_schema — schema self-validation
# ---------------------------------------------------------------------------


def test_validate_df_eval_schema_passes_valid_schema():
    """Valid schema with unique alias sources passes without raising."""
    schema = pa.DataFrameSchema(
        {
            "legacy_price": pa.Column(float),
            "price": pa.Column(float, metadata={"df-eval": {"alias": "legacy_price"}}),
            "taxed": pa.Column(float, metadata={"df-eval": {"expr": "price * 1.075"}}),
        }
    )
    validate_df_eval_schema(schema)  # should not raise


def test_validate_df_eval_schema_rejects_self_alias():
    """Schema where an alias target lists itself as a source raises ValueError."""
    schema = pa.DataFrameSchema(
        {
            "price": pa.Column(float, metadata={"df-eval": {"alias": "price"}}),
        }
    )
    with pytest.raises(ValueError, match="cannot list itself as a source"):
        validate_df_eval_schema(schema)


def test_validate_df_eval_schema_rejects_duplicate_source():
    """Schema with two targets sharing the same alias source raises ValueError."""
    schema = pa.DataFrameSchema(
        {
            "deposit": pa.Column(float),
            "deposit_code": pa.Column(
                float, metadata={"df-eval": {"alias": "deposit"}}
            ),
            "deposit_amount": pa.Column(
                float, metadata={"df-eval": {"alias": "deposit"}}
            ),
        }
    )
    with pytest.raises(ValueError, match="alias sources must be unique"):
        validate_df_eval_schema(schema)


def test_validate_df_eval_schema_rejects_unknown_source():
    """Schema with alias source not in schema columns raises ValueError."""
    schema = pa.DataFrameSchema(
        {
            "price": pa.Column(float, metadata={"df-eval": {"alias": "nonexistent"}}),
        }
    )
    with pytest.raises(ValueError, match="is not a column defined in the schema"):
        validate_df_eval_schema(schema)


def test_validate_df_eval_schema_rejects_alias_with_operation():
    """Schema column combining alias and expr raises ValueError."""
    schema = pa.DataFrameSchema(
        {
            "legacy": pa.Column(float),
            "price": pa.Column(
                float,
                metadata={"df-eval": {"alias": "legacy", "expr": "legacy * 2"}},
            ),
        }
    )
    with pytest.raises(ValueError, match="cannot define both"):
        validate_df_eval_schema(schema)


def test_validate_df_eval_schema_passes_multi_source_aliases():
    """Schema with multi-source aliases (list) passes when sources are unique."""
    schema = pa.DataFrameSchema(
        {
            "deposit": pa.Column(float),
            "dep": pa.Column(float),
            "deposit_code": pa.Column(
                float,
                metadata={"df-eval": {"alias": ["deposit", "dep"]}},
            ),
        }
    )
    validate_df_eval_schema(schema)  # should not raise


def test_validate_df_eval_schema_rejects_duplicate_multi_source():
    """Shared source across multi-source lists raises ValueError."""
    schema = pa.DataFrameSchema(
        {
            "deposit": pa.Column(float),
            "deposit_code": pa.Column(
                float, metadata={"df-eval": {"alias": ["deposit"]}}
            ),
            "deposit_amount": pa.Column(
                float, metadata={"df-eval": {"alias": ["deposit"]}}
            ),
        }
    )
    with pytest.raises(ValueError, match="alias sources must be unique"):
        validate_df_eval_schema(schema)


# ---------------------------------------------------------------------------
# Parquet path with aliases
# ---------------------------------------------------------------------------


def test_apply_pandera_schema_parquet_with_alias_rename(tmp_path):
    """Parquet path should rename alias source columns per-chunk before evaluation."""
    pa_arrow = pytest.importorskip("pyarrow")
    pq_arrow = pytest.importorskip("pyarrow.parquet")

    schema = pa.DataFrameSchema(
        {
            "legacy_price": pa.Column(float),
            "price": pa.Column(float, metadata={"df-eval": {"alias": "legacy_price"}}),
            "taxed": pa.Column(float, metadata={"df-eval": {"expr": "price * 1.075"}}),
        }
    )

    # Parquet file uses the alias SOURCE name (legacy_price), not the target (price).
    input_df = pd.DataFrame({"legacy_price": [10.0, 20.0]})
    input_path = tmp_path / "alias_in.parquet"
    output_path = tmp_path / "alias_out.parquet"
    pq_arrow.write_table(
        pa_arrow.Table.from_pandas(input_df, preserve_index=False), input_path
    )

    result_path = apply_pandera_schema_parquet_to_parquet(
        input_path, output_path, schema, chunk_size=1
    )

    assert result_path == output_path
    output_df = pd.read_parquet(output_path)
    assert list(output_df.columns) == ["legacy_price", "price", "taxed"]
    assert list(output_df["price"]) == [10.0, 20.0]
    assert pytest.approx(list(output_df["taxed"])) == [10.75, 21.5]


def test_apply_pandera_schema_parquet_with_native_alias(tmp_path):
    """Parquet path should handle NATIVE aliases (target already in file)."""
    pa_arrow = pytest.importorskip("pyarrow")
    pq_arrow = pytest.importorskip("pyarrow.parquet")

    schema = pa.DataFrameSchema(
        {
            "legacy_price": pa.Column(float),
            "price": pa.Column(float, metadata={"df-eval": {"alias": "legacy_price"}}),
            "taxed": pa.Column(float, metadata={"df-eval": {"expr": "price * 1.075"}}),
        }
    )

    # Parquet file uses the alias TARGET name (price) directly.
    input_df = pd.DataFrame({"price": [10.0, 20.0]})
    input_path = tmp_path / "native_in.parquet"
    output_path = tmp_path / "native_out.parquet"
    pq_arrow.write_table(
        pa_arrow.Table.from_pandas(input_df, preserve_index=False), input_path
    )

    result_path = apply_pandera_schema_parquet_to_parquet(
        input_path, output_path, schema, chunk_size=1
    )

    assert result_path == output_path
    output_df = pd.read_parquet(output_path)
    assert "price" in output_df.columns
    assert "taxed" in output_df.columns
    assert list(output_df["price"]) == [10.0, 20.0]
    assert pytest.approx(list(output_df["taxed"])) == [10.75, 21.5]


def test_apply_pandera_schema_parquet_ambiguous_alias_raises(tmp_path):
    """Parquet path should raise on AMBIGUOUS alias (both source and target present)."""
    pa_arrow = pytest.importorskip("pyarrow")
    pq_arrow = pytest.importorskip("pyarrow.parquet")

    schema = pa.DataFrameSchema(
        {
            "legacy_price": pa.Column(float),
            "price": pa.Column(float, metadata={"df-eval": {"alias": "legacy_price"}}),
            "taxed": pa.Column(float, metadata={"df-eval": {"expr": "price * 1.075"}}),
        }
    )

    input_df = pd.DataFrame({"legacy_price": [10.0], "price": [9.0]})
    input_path = tmp_path / "ambiguous_in.parquet"
    pq_arrow.write_table(
        pa_arrow.Table.from_pandas(input_df, preserve_index=False), input_path
    )

    with pytest.raises(ValueError, match="alias target and source columns"):
        apply_pandera_schema_parquet_to_parquet(
            input_path, tmp_path / "out.parquet", schema
        )

