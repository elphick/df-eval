"""Tests for Expression parsing and dependency extraction."""

import pytest
from df_eval.expr import Expression


def test_expression_parse_static_method():
    """Test Expression.parse static method."""
    expr = Expression.parse("a + b")
    assert isinstance(expr, Expression)
    assert expr.expr_str == "a + b"


def test_expression_parse_invalid():
    """Test Expression.parse with invalid expression."""
    # This should still create an expression, just with empty dependencies
    expr = Expression.parse("a +")
    assert isinstance(expr, Expression)


def test_expression_dependencies_simple():
    """Test dependency extraction for simple expressions."""
    expr = Expression("a + b")
    assert "a" in expr.dependencies
    assert "b" in expr.dependencies
    assert len(expr.dependencies) == 2


def test_expression_dependencies_complex():
    """Test dependency extraction for complex expressions."""
    expr = Expression("a * b + c / d")
    assert expr.dependencies == {"a", "b", "c", "d"}


def test_expression_dependencies_with_functions():
    """Test dependency extraction with function calls."""
    expr = Expression("sqrt(a) + log(b)")
    assert "a" in expr.dependencies
    assert "b" in expr.dependencies
    # Note: AST treats function names as variables too, which is acceptable
    # since our engine will handle them appropriately


def test_expression_dependencies_no_variables():
    """Test dependency extraction with no variables."""
    expr = Expression("5 + 10")
    assert len(expr.dependencies) == 0


def test_expression_dependencies_repeated():
    """Test that dependencies don't count duplicates."""
    expr = Expression("a + a * a")
    assert expr.dependencies == {"a"}
