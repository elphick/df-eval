"""Tests for the engine module."""

import pytest
import pandas as pd
import numpy as np
from df_eval.engine import Engine
from df_eval.expr import Expression


def test_engine_init():
    """Test Engine initialization."""
    engine = Engine()
    assert engine.functions is not None
    assert len(engine.functions) > 0


def test_engine_evaluate_simple():
    """Test Engine evaluate with simple expression."""
    engine = Engine()
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    result = engine.evaluate(df, "a + b")
    expected = pd.Series([5, 7, 9], name="a + b")
    pd.testing.assert_series_equal(result, expected)


def test_engine_evaluate_with_expression_object():
    """Test Engine evaluate with Expression object."""
    engine = Engine()
    df = pd.DataFrame({"x": [10, 20, 30]})
    expr = Expression("x * 2")
    result = engine.evaluate(df, expr)
    expected = pd.Series([20, 40, 60], name="x * 2")
    pd.testing.assert_series_equal(result, expected)


def test_engine_evaluate_invalid_expression():
    """Test Engine evaluate with invalid expression."""
    engine = Engine()
    df = pd.DataFrame({"a": [1, 2, 3]})
    with pytest.raises(ValueError):
        engine.evaluate(df, "a + nonexistent_column")


def test_engine_register_function():
    """Test registering a custom function."""
    engine = Engine()
    
    def custom_func(x):
        return x * 10
    
    engine.register_function("custom", custom_func)
    assert "custom" in engine.functions
    assert engine.functions["custom"] == custom_func


def test_engine_apply_schema():
    """Test applying a schema to a DataFrame."""
    engine = Engine()
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    schema = {
        "sum": "a + b",
        "product": "a * b"
    }
    result = engine.apply_schema(df, schema)
    
    assert "sum" in result.columns
    assert "product" in result.columns
    assert list(result["sum"]) == [5, 7, 9]
    assert list(result["product"]) == [4, 10, 18]


def test_engine_apply_schema_does_not_modify_original():
    """Test that apply_schema doesn't modify the original DataFrame."""
    engine = Engine()
    df = pd.DataFrame({"a": [1, 2, 3]})
    original_columns = df.columns.tolist()
    
    schema = {"b": "a * 2"}
    result = engine.apply_schema(df, schema)
    
    assert df.columns.tolist() == original_columns
    assert "b" not in df.columns
    assert "b" in result.columns
