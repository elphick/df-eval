"""
Built-in functions for expression evaluation.

This module provides built-in functions that can be used in expressions.
These are safe, vectorized functions that are allow-listed for use in expressions.
"""

import numpy as np
import pandas as pd
from typing import Any, Callable


def safe_divide(a: Any, b: Any) -> Any:
    """
    Safely divide two values, returning NaN for division by zero.
    
    Args:
        a: The numerator.
        b: The denominator.
        
    Returns:
        The result of a / b, or NaN if b is zero.
    """
    with np.errstate(divide='ignore', invalid='ignore'):
        return np.where(b != 0, np.divide(a, b), np.nan)


def coalesce(*args: Any) -> Any:
    """
    Return the first non-null value from the arguments.
    
    Args:
        *args: Values to check.
        
    Returns:
        The first non-null value, or None if all are null.
    """
    for arg in args:
        if arg is not None and (not isinstance(arg, float) or not np.isnan(arg)):
            return arg
    return None


def clip_value(value: Any, min_val: float | None = None, max_val: float | None = None) -> Any:
    """
    Clip values to a specified range.
    
    Args:
        value: The value to clip.
        min_val: The minimum value (optional).
        max_val: The maximum value (optional).
        
    Returns:
        The clipped value.
    """
    if min_val is not None:
        value = np.maximum(value, min_val)
    if max_val is not None:
        value = np.minimum(value, max_val)
    return value


def safe_abs(value: Any) -> Any:
    """Absolute value function."""
    return np.abs(value)


def safe_log(value: Any) -> Any:
    """Natural logarithm function."""
    with np.errstate(divide='ignore', invalid='ignore'):
        return np.log(value)


def safe_exp(value: Any) -> Any:
    """Exponential function."""
    with np.errstate(over='ignore'):
        return np.exp(value)


def safe_sqrt(value: Any) -> Any:
    """Square root function."""
    with np.errstate(invalid='ignore'):
        return np.sqrt(value)


def safe_clip(value: Any, a_min: Any, a_max: Any) -> Any:
    """Clip values to a range."""
    return np.clip(value, a_min, a_max)


def safe_where(condition: Any, x: Any, y: Any) -> Any:
    """Return elements from x or y depending on condition."""
    return np.where(condition, x, y)


def safe_isna(value: Any) -> Any:
    """Check for NaN/None values."""
    if isinstance(value, pd.Series):
        return value.isna()
    return pd.isna(value)


def safe_fillna(value: Any, fill_value: Any) -> Any:
    """Fill NaN/None values with a specified value."""
    if isinstance(value, pd.Series):
        return value.fillna(fill_value)
    return fill_value if pd.isna(value) else value


# Dictionary of allow-listed safe functions available for expressions
BUILTIN_FUNCTIONS: dict[str, Callable] = {
    "safe_divide": safe_divide,
    "coalesce": coalesce,
    "clip": clip_value,
    # Allow-listed safe functions
    "abs": safe_abs,
    "log": safe_log,
    "exp": safe_exp,
    "sqrt": safe_sqrt,
    "clip": safe_clip,
    "where": safe_where,
    "isna": safe_isna,
    "fillna": safe_fillna,
}
