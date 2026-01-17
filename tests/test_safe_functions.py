"""Tests for the new allow-listed safe functions."""

import pytest
import pandas as pd
import numpy as np
from df_eval.functions import (
    safe_abs, safe_log, safe_exp, safe_sqrt, safe_clip,
    safe_where, safe_isna, safe_fillna
)


def test_safe_abs():
    """Test safe_abs function."""
    assert safe_abs(-5) == 5
    assert safe_abs(3.5) == 3.5
    result = safe_abs(np.array([-1, -2, 3]))
    np.testing.assert_array_equal(result, [1, 2, 3])


def test_safe_log():
    """Test safe_log function."""
    assert np.isclose(safe_log(np.e), 1.0)
    assert np.isnan(safe_log(-1))
    result = safe_log(np.array([1, np.e, np.e**2]))
    assert np.isclose(result[0], 0.0)
    assert np.isclose(result[1], 1.0)
    assert np.isclose(result[2], 2.0)


def test_safe_exp():
    """Test safe_exp function."""
    assert np.isclose(safe_exp(0), 1.0)
    assert np.isclose(safe_exp(1), np.e)
    result = safe_exp(np.array([0, 1, 2]))
    assert np.isclose(result[0], 1.0)


def test_safe_sqrt():
    """Test safe_sqrt function."""
    assert safe_sqrt(4) == 2.0
    assert safe_sqrt(9) == 3.0
    assert np.isnan(safe_sqrt(-1))
    result = safe_sqrt(np.array([0, 1, 4, 9]))
    np.testing.assert_array_equal(result, [0, 1, 2, 3])


def test_safe_clip():
    """Test safe_clip function."""
    assert safe_clip(5, 0, 10) == 5
    assert safe_clip(-5, 0, 10) == 0
    assert safe_clip(15, 0, 10) == 10
    result = safe_clip(np.array([-5, 5, 15]), 0, 10)
    np.testing.assert_array_equal(result, [0, 5, 10])


def test_safe_where():
    """Test safe_where function."""
    condition = np.array([True, False, True])
    x = np.array([1, 2, 3])
    y = np.array([4, 5, 6])
    result = safe_where(condition, x, y)
    np.testing.assert_array_equal(result, [1, 5, 3])


def test_safe_isna():
    """Test safe_isna function."""
    assert safe_isna(None) == True
    assert safe_isna(np.nan) == True
    assert safe_isna(5) == False
    
    series = pd.Series([1, np.nan, 3, None])
    result = safe_isna(series)
    expected = pd.Series([False, True, False, True])
    pd.testing.assert_series_equal(result, expected)


def test_safe_fillna():
    """Test safe_fillna function."""
    assert safe_fillna(5, 0) == 5
    assert safe_fillna(np.nan, 0) == 0
    assert safe_fillna(None, 0) == 0
    
    series = pd.Series([1, np.nan, 3, None])
    result = safe_fillna(series, 0)
    expected = pd.Series([1.0, 0.0, 3.0, 0.0])  # Keep as float64
    pd.testing.assert_series_equal(result, expected)
