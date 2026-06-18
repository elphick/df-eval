"""Tests for the functions module."""

import pytest
import numpy as np
import pandas as pd
from df_eval.functions import (
    safe_divide,
    coalesce,
    clip_value,
    safe_round,
    safe_ceil,
    safe_floor,
    BUILTIN_FUNCTIONS,
)


def test_safe_divide_normal():
    """Test safe_divide with normal values."""
    result = safe_divide(10, 2)
    assert result == 5.0


def test_safe_divide_by_zero():
    """Test safe_divide with zero denominator."""
    result = safe_divide(10, 0)
    assert np.isnan(result)


def test_safe_divide_arrays():
    """Test safe_divide with numpy arrays."""
    a = np.array([10, 20, 30])
    b = np.array([2, 0, 5])
    result = safe_divide(a, b)
    assert result[0] == 5.0
    assert np.isnan(result[1])
    assert result[2] == 6.0


def test_coalesce_first_non_null():
    """Test coalesce returns first non-null value."""
    result = coalesce(None, 5, 10)
    assert result == 5


def test_coalesce_all_null():
    """Test coalesce with all null values."""
    result = coalesce(None, None, None)
    assert result is None


def test_coalesce_with_nan():
    """Test coalesce handles NaN values."""
    result = coalesce(np.nan, 42, 100)
    assert result == 42


def test_coalesce_first_valid():
    """Test coalesce returns first value when valid."""
    result = coalesce(1, 2, 3)
    assert result == 1


def test_clip_value_no_limits():
    """Test clip_value with no limits."""
    result = clip_value(5)
    assert result == 5


def test_clip_value_min_only():
    """Test clip_value with minimum only."""
    result = clip_value(3, min_val=5)
    assert result == 5


def test_clip_value_max_only():
    """Test clip_value with maximum only."""
    result = clip_value(10, max_val=7)
    assert result == 7


def test_clip_value_both_limits():
    """Test clip_value with both min and max."""
    assert clip_value(3, min_val=5, max_val=10) == 5
    assert clip_value(7, min_val=5, max_val=10) == 7
    assert clip_value(12, min_val=5, max_val=10) == 10


def test_builtin_functions_dict():
    """Test that BUILTIN_FUNCTIONS contains expected functions."""
    assert "safe_divide" in BUILTIN_FUNCTIONS
    assert "coalesce" in BUILTIN_FUNCTIONS
    assert "clip" in BUILTIN_FUNCTIONS
    assert callable(BUILTIN_FUNCTIONS["safe_divide"])
    assert callable(BUILTIN_FUNCTIONS["coalesce"])
    assert callable(BUILTIN_FUNCTIONS["clip"])
    assert "round" in BUILTIN_FUNCTIONS
    assert "ceil" in BUILTIN_FUNCTIONS
    assert "floor" in BUILTIN_FUNCTIONS


def test_safe_round_scalar_and_null():
    """safe_round should support scalar numeric values and nulls."""
    assert safe_round(12.3456, 2) == 12.35
    assert safe_round(None, 2) is None
    assert np.isnan(safe_round(np.nan, 2))


def test_safe_round_series_preserves_nan_and_dtype():
    """safe_round should be vectorized for Series and keep missing values."""
    series = pd.Series([1.234, np.nan, 2.345], dtype="float64")
    rounded = safe_round(series, 2)

    expected = pd.Series([1.23, np.nan, 2.35], dtype="float64")
    pd.testing.assert_series_equal(rounded, expected)


def test_safe_ceil_and_floor_scalar_and_series():
    """Ceil and floor should work for both scalars and Series."""
    assert safe_ceil(1.2) == 2.0
    assert safe_floor(1.8) == 1.0
    assert safe_ceil(None) is None
    assert np.isnan(safe_floor(np.nan))

    series = pd.Series([1.1, np.nan, -1.1], dtype="float64")
    ceiled = safe_ceil(series)
    floored = safe_floor(series)

    pd.testing.assert_series_equal(
        ceiled,
        pd.Series([2.0, np.nan, -1.0], dtype="float64"),
    )
    pd.testing.assert_series_equal(
        floored,
        pd.Series([1.0, np.nan, -2.0], dtype="float64"),
    )
