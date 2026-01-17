"""
Built-in functions for expression evaluation.

This module provides built-in functions that can be used in expressions.
"""

import numpy as np
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
    return np.divide(a, b, out=np.full_like(a, np.nan, dtype=float), where=(b != 0))


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


# Dictionary of built-in functions available for expressions
BUILTIN_FUNCTIONS: dict[str, Callable] = {
    "safe_divide": safe_divide,
    "coalesce": coalesce,
    "clip": clip_value,
}
