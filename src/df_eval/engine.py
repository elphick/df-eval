"""
Evaluation engine module.

This module provides the Engine class for evaluating expressions
on pandas DataFrames.
"""

import pandas as pd
from typing import Any, Callable

from df_eval.expr import Expression
from df_eval.functions import BUILTIN_FUNCTIONS


class Engine:
    """
    Engine for evaluating expressions on pandas DataFrames.
    
    The Engine class provides methods to evaluate expressions and
    apply transformations to DataFrames.
    """
    
    def __init__(self) -> None:
        """Initialize the evaluation engine."""
        self.functions = BUILTIN_FUNCTIONS.copy()
    
    def evaluate(self, df: pd.DataFrame, expr: str | Expression) -> Any:
        """
        Evaluate an expression on a DataFrame.
        
        Args:
            df: The DataFrame to evaluate the expression on.
            expr: The expression to evaluate (string or Expression object).
            
        Returns:
            The result of evaluating the expression.
            
        Raises:
            ValueError: If the expression is invalid.
        """
        if isinstance(expr, str):
            expr = Expression(expr)
        
        # Use pandas eval for basic expressions
        try:
            return df.eval(expr.expr_str)
        except Exception as e:
            raise ValueError(f"Failed to evaluate expression: {e}") from e
    
    def register_function(self, name: str, func: Callable[..., Any]) -> None:
        """
        Register a custom function for use in expressions.
        
        Args:
            name: The name to register the function under.
            func: The function to register.
        """
        self.functions[name] = func
    
    def apply_schema(self, df: pd.DataFrame, schema: dict[str, str]) -> pd.DataFrame:
        """
        Apply a schema of derived columns to a DataFrame.
        
        Args:
            df: The input DataFrame.
            schema: A dictionary mapping column names to expressions.
            
        Returns:
            A new DataFrame with the derived columns added.
        """
        result = df.copy()
        for col_name, expr_str in schema.items():
            result[col_name] = self.evaluate(result, expr_str)
        return result
