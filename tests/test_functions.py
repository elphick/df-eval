"""Tests for the functions module."""

import pytest
import numpy as np
from df_eval.functions import safe_divide, coalesce, clip_value, BUILTIN_FUNCTIONS


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
