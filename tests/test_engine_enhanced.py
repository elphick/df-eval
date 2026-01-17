"""Tests for enhanced Engine features."""

import pytest
import pandas as pd
import numpy as np
from df_eval.engine import Engine, CycleDetectedError
from df_eval.expr import Expression


def test_engine_register_constant():
    """Test registering constants."""
    engine = Engine()
    engine.register_constant("PI", 3.14159)
    engine.register_constant("E", 2.71828)
    
    df = pd.DataFrame({"x": [1, 2, 3]})
    result = engine.evaluate(df, "x * PI")
    expected = pd.Series([3.14159, 6.28318, 9.42477])
    pd.testing.assert_series_equal(result, expected, check_names=False)


def test_engine_evaluate_with_dtype():
    """Test evaluate with dtype casting."""
    engine = Engine()
    df = pd.DataFrame({"a": [1.5, 2.7, 3.9]})
    result = engine.evaluate(df, "a", dtype="int")
    assert result.dtype == np.int64


def test_engine_evaluate_many():
    """Test evaluate_many method."""
    engine = Engine()
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    
    expressions = {
        "sum": "a + b",
        "product": "a * b"
    }
    
    result = engine.evaluate_many(df, expressions)
    assert "sum" in result.columns
    assert "product" in result.columns
    assert list(result["sum"]) == [5, 7, 9]
    assert list(result["product"]) == [4, 10, 18]


def test_engine_apply_schema_with_dependencies():
    """Test apply_schema with dependencies between columns."""
    engine = Engine()
    df = pd.DataFrame({"a": [1, 2, 3]})
    
    schema = {
        "b": "a * 2",
        "c": "b + 10",  # Depends on b
        "d": "c * a"     # Depends on both c and a
    }
    
    result = engine.apply_schema(df, schema)
    assert list(result["b"]) == [2, 4, 6]
    assert list(result["c"]) == [12, 14, 16]
    assert list(result["d"]) == [12, 28, 48]


def test_engine_apply_schema_cycle_detection():
    """Test that cycles are detected."""
    engine = Engine()
    df = pd.DataFrame({"a": [1, 2, 3]})
    
    schema = {
        "b": "c + 1",  # Depends on c
        "c": "b + 1"   # Depends on b - creates a cycle
    }
    
    with pytest.raises(CycleDetectedError):
        engine.apply_schema(df, schema)


def test_engine_apply_schema_with_dtypes():
    """Test apply_schema with dtype specifications."""
    engine = Engine()
    df = pd.DataFrame({"a": [1.5, 2.7, 3.9]})
    
    schema = {"b": "a * 2"}
    dtypes = {"b": "int"}
    
    result = engine.apply_schema(df, schema, dtypes=dtypes)
    assert result["b"].dtype == np.int64


def test_engine_provenance_tracking():
    """Test provenance tracking in df.attrs."""
    engine = Engine()
    engine.enable_provenance(True)
    
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    schema = {
        "sum": "a + b",
        "product": "a * b"
    }
    
    result = engine.apply_schema(df, schema)
    
    assert 'df_eval_provenance' in result.attrs
    assert 'sum' in result.attrs['df_eval_provenance']
    assert result.attrs['df_eval_provenance']['sum']['expression'] == 'a + b'
    assert set(result.attrs['df_eval_provenance']['sum']['dependencies']) == {'a', 'b'}


def test_engine_topological_sort_complex():
    """Test topological sort with complex dependencies."""
    engine = Engine()
    df = pd.DataFrame({"x": [1, 2, 3]})
    
    schema = {
        "a": "x * 2",
        "b": "x + 1",
        "c": "a + b",
        "d": "c * 2",
        "e": "a + d"
    }
    
    result = engine.apply_schema(df, schema)
    
    # Verify all columns are present
    for col in schema.keys():
        assert col in result.columns
    
    # Verify values are correct
    assert list(result["a"]) == [2, 4, 6]
    assert list(result["b"]) == [2, 3, 4]
    assert list(result["c"]) == [4, 7, 10]
    assert list(result["d"]) == [8, 14, 20]
    assert list(result["e"]) == [10, 18, 26]
