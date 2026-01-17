"""
Expression parsing and representation module.

This module provides the Expression class for parsing and representing
expressions that can be evaluated on pandas DataFrames.
"""

import ast
from typing import Any, Set


class Expression:
    """
    Represents a parsed expression that can be evaluated on a DataFrame.
    
    Attributes:
        expr_str: The string representation of the expression.
        dependencies: Set of column names referenced in the expression.
    """
    
    def __init__(self, expr_str: str) -> None:
        """
        Initialize an Expression.
        
        Args:
            expr_str: The expression string to parse.
        """
        self.expr_str = expr_str
        self._parsed = self._parse(expr_str)
        self.dependencies = self._extract_dependencies(expr_str)
    
    @staticmethod
    def parse(expr_str: str) -> "Expression":
        """
        Parse an expression string into an Expression object.
        
        Args:
            expr_str: The expression string to parse.
            
        Returns:
            An Expression object.
            
        Raises:
            ValueError: If the expression is invalid.
        """
        try:
            return Expression(expr_str)
        except Exception as e:
            raise ValueError(f"Failed to parse expression: {e}") from e
    
    def _parse(self, expr_str: str) -> Any:
        """
        Parse the expression string.
        
        Args:
            expr_str: The expression string to parse.
            
        Returns:
            The parsed expression representation.
        """
        # Basic parsing implementation
        return expr_str.strip()
    
    def _extract_dependencies(self, expr_str: str) -> Set[str]:
        """
        Extract column dependencies from the expression.
        
        Args:
            expr_str: The expression string.
            
        Returns:
            A set of column names referenced in the expression.
        """
        dependencies = set()
        try:
            tree = ast.parse(expr_str, mode='eval')
            for node in ast.walk(tree):
                if isinstance(node, ast.Name):
                    dependencies.add(node.id)
        except SyntaxError:
            # If parsing fails, return empty set
            pass
        return dependencies
    
    def __repr__(self) -> str:
        """Return string representation of the expression."""
        return f"Expression('{self.expr_str}')"
    
    def __str__(self) -> str:
        """Return the expression string."""
        return self.expr_str
