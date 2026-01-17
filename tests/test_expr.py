"""Tests for the expr module."""

import pytest
from df_eval.expr import Expression


def test_expression_init():
    """Test Expression initialization."""
    expr = Expression("a + b")
    assert expr.expr_str == "a + b"


def test_expression_str():
    """Test Expression string representation."""
    expr = Expression("x * 2")
    assert str(expr) == "x * 2"


def test_expression_repr():
    """Test Expression repr."""
    expr = Expression("y - 1")
    assert repr(expr) == "Expression('y - 1')"


def test_expression_parse_strips_whitespace():
    """Test that Expression parsing strips whitespace."""
    expr = Expression("  a + b  ")
    assert expr._parsed == "a + b"
