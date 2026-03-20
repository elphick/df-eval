"""
Evaluation engine module.

This module provides the Engine class for evaluating expressions
on pandas DataFrames with support for UDF registry, schema-driven
derived columns with topological ordering, and provenance tracking.
"""

from collections.abc import Iterator, Sequence
from pathlib import Path

import pandas as pd
from typing import Any, Callable, Dict, List, Optional, Set

from df_eval.expr import Expression
from df_eval.functions import BUILTIN_FUNCTIONS
from df_eval.parquet import iter_parquet_row_chunks, write_parquet_row_chunks
from df_eval.lookup import Resolver, lookup as _lookup


class CycleDetectedError(Exception):
    """Raised when a cycle is detected in column dependencies."""
    pass


class Engine:
    """
    Engine for evaluating expressions on pandas DataFrames.
    
    The Engine class provides methods to evaluate expressions,
    apply transformations, and manage UDF/constant registries.
    """
    
    def __init__(self) -> None:
        """Initialize the evaluation engine."""
        self.functions = BUILTIN_FUNCTIONS.copy()
        self.constants: Dict[str, Any] = {}
        # Registry of external lookup resolvers (e.g., DictResolver instances)
        # that can be referenced by name from expressions via the ``lookup``
        # helper function.
        self.resolvers: Dict[str, Resolver] = {}
        # Registry of metadata-driven pipeline functions. These functions are
        # invoked by higher-level orchestration (e.g., Pandera integration)
        # rather than directly from pandas.eval expressions.
        self.pipeline_functions: Dict[str, Callable[..., Any]] = {}
        self._track_provenance = False
    
    def enable_provenance(self, enabled: bool = True) -> None:
        """
        Enable or disable provenance tracking.
        
        Args:
            enabled: Whether to track provenance in df.attrs.
        """
        self._track_provenance = enabled
    
    def register_function(self, name: str, func: Callable[..., Any]) -> None:
        """
        Register a custom function (UDF) for use in expressions.
        
        Args:
            name: The name to register the function under.
            func: The function to register.
        """
        self.functions[name] = func
    
    def register_constant(self, name: str, value: Any) -> None:
        """
        Register a constant for use in expressions.
        
        Args:
            name: The name to register the constant under.
            value: The constant value.
        """
        self.constants[name] = value

    def register_resolver(self, name: str, resolver: Resolver) -> None:
        """Register a lookup resolver for use in expressions.

        Registered resolvers can be referenced by name from expressions via
        the :func:`lookup` helper, for example::

            engine.register_resolver("prices", price_resolver)
            schema = {"price": "lookup(product, prices)"}

        Args:
            name: Name to register the resolver under.
            resolver: Resolver instance (e.g., :class:`DictResolver`).
        """
        self.resolvers[name] = resolver

    def register_pipeline_function(self, name: str, func: Callable[..., Any]) -> None:
        """Register a named pipeline function for metadata-driven workflows.

        Pipeline functions are invoked by higher-level orchestration layers
        (for example, Pandera-driven schemas) based on column metadata rather
        than being called directly from df-eval expression strings. A pipeline
        function typically accepts a ``pandas.DataFrame`` slice and optional
        keyword arguments, and returns either a ``Series`` or ``DataFrame``
        aligned with the input index.
        """
        self.pipeline_functions[name] = func
    
    def evaluate(
        self, 
        df: pd.DataFrame, 
        expr: str | Expression,
        dtype: Optional[str] = None
    ) -> Any:
        """
        Evaluate an expression on a DataFrame.
        
        Args:
            df: The DataFrame to evaluate the expression on.
            expr: The expression to evaluate (string or Expression object).
            dtype: Optional dtype to cast the result to.
            
        Returns:
            The result of evaluating the expression.
            
        Raises:
            ValueError: If the expression is invalid.
        """
        if isinstance(expr, str):
            expr = Expression(expr)

        # Use pandas eval for expressions
        try:
            # Pass constants as resolvers in the evaluation so they behave
            # like variables in the expression namespace.
            result = df.eval(
                expr.expr_str,
                resolvers=[self.constants, self.functions],
            )
            
            # Apply dtype cast if specified
            if dtype is not None and isinstance(result, pd.Series):
                result = result.astype(dtype)
            
            return result
        except Exception as e:
            raise ValueError(f"Failed to evaluate expression '{expr.expr_str}': {e}") from e
    
    def evaluate_many(
        self,
        df: pd.DataFrame,
        expressions: Dict[str, str | Expression]
    ) -> pd.DataFrame:
        """
        Evaluate multiple expressions and add them as columns.
        
        This is an alias for apply_schema for batch evaluation.
        
        Args:
            df: The input DataFrame.
            expressions: A dictionary mapping column names to expressions.
            
        Returns:
            A new DataFrame with the evaluated columns added.
        """
        return self.apply_schema(df, expressions)
    
    def apply_schema(
        self,
        df: pd.DataFrame,
        schema: Dict[str, str | Expression],
        dtypes: Optional[Dict[str, str]] = None
    ) -> pd.DataFrame:
        """
        Apply a schema of derived columns to a DataFrame with topological ordering.
        
        This method automatically handles dependencies between columns and
        detects cycles in the dependency graph.
        
        Args:
            df: The input DataFrame.
            schema: A dictionary mapping column names to expressions.
            dtypes: Optional dictionary mapping column names to dtypes.
            
        Returns:
            A new DataFrame with the derived columns added.
            
        Raises:
            CycleDetectedError: If a cycle is detected in dependencies.
        """
        result = df.copy()
        dtypes = dtypes or {}
        
        # Track provenance if enabled
        if self._track_provenance:
            if 'df_eval_provenance' not in result.attrs:
                result.attrs['df_eval_provenance'] = {}
        
        # Convert all to Expression objects and build dependency graph
        expr_objects: Dict[str, Expression] = {}
        for col_name, expr in schema.items():
            if isinstance(expr, str):
                expr_objects[col_name] = Expression(expr)
            else:
                expr_objects[col_name] = expr
        
        # Perform topological sort
        ordered_cols = self._topological_sort(expr_objects, set(result.columns))
        
        # Evaluate in dependency order
        for col_name in ordered_cols:
            expr_obj = expr_objects[col_name]
            dtype = dtypes.get(col_name)
            result[col_name] = self.evaluate(result, expr_obj, dtype=dtype)
            
            # Track provenance
            if self._track_provenance:
                result.attrs['df_eval_provenance'][col_name] = {
                    'expression': expr_obj.expr_str,
                    'dependencies': list(expr_obj.dependencies)
                }
        
        return result

    def apply_operations(
        self,
        df: pd.DataFrame,
        operations: Dict[str, Dict[str, Any]],
        dtypes: Optional[Dict[str, str]] = None,
    ) -> pd.DataFrame:
        """Apply a set of operations (expr, lookup, function) to a DataFrame.

        ``operations`` is a mapping from column name to a spec with keys::

            {
                "kind": "expr" | "lookup" | "function",
                "expr": str | None,
                "lookup": dict | None,
                "function": dict | None,
            }

        This is intended to be used by higher-level integrations such as the
        Pandera helpers, which translate column metadata into this structure.
        """
        result = df.copy()
        dtypes = dtypes or {}

        # Build Expression objects only for expr-kind operations so we can
        # reuse the existing dependency analysis and topological sort.
        expr_objects: Dict[str, Expression] = {}
        for col_name, op in operations.items():
            if op.get("kind") == "expr":
                expr_str = op.get("expr")
                expr_objects[col_name] = Expression(expr_str)

        # Compute a dependency-aware ordering for *all* operation outputs by
        # treating expr outputs as derived columns that must respect their
        # dependencies (which may include lookup/function outputs).
        # We start from the union of the current columns and all operation
        # output columns so that the topological sort includes all expr nodes.
        existing_cols = set(result.columns)
        all_cols = existing_cols.union(expr_objects.keys())
        ordered_expr_cols = self._topological_sort(expr_objects, existing_cols)

        # Start with a stable order: first any non-expr operations in the
        # dictionary order, then expr operations in dependency order.
        ordered_ops: list[str] = []

        # 1) Apply all lookup operations first so that any resolved columns
        #    (e.g. "price") are available to subsequent expressions and
        #    functions regardless of dictionary ordering.
        for name, spec in operations.items():
            if spec.get("kind") == "lookup":
                ordered_ops.append(name)

        # 2) Apply expr operations in dependency order. These may consume
        #    lookup-generated or original columns and produce intermediate
        #    columns (e.g. "line_total") that functions can depend on.
        for col in ordered_expr_cols:
            if col in operations:
                ordered_ops.append(col)

        # 3) Finally, apply function operations. Functions can depend on
        #    both expr outputs and lookup-generated columns.
        for name, spec in operations.items():
            if spec.get("kind") == "function":
                ordered_ops.append(name)

        # Apply operations in the computed order. Lookups and functions
        # materialize their columns on ``result``, which may then be consumed
        # by later expr operations or pipeline functions.
        for col_name in ordered_ops:
            op = operations[col_name]
            kind = op.get("kind")

            if kind == "lookup":
                lookup_spec = op.get("lookup") or {}
                series = self._apply_lookup_operation(result, lookup_spec)
                result[col_name] = series

            elif kind == "function":
                func_spec = op.get("function") or {}
                result = self._apply_pipeline_function(result, func_spec)

            elif kind == "expr":
                expr_obj = expr_objects[col_name]
                dtype = dtypes.get(col_name)
                result[col_name] = self.evaluate(result, expr_obj, dtype=dtype)

        return result

    # ------------------------------------------------------------------
    # Metadata-driven pipeline helpers
    # ------------------------------------------------------------------

    def _apply_pipeline_function(self, df: pd.DataFrame, spec: Dict[str, Any]) -> pd.DataFrame:
        """Apply a registered pipeline function according to a metadata spec.

        The spec supports the following keys::

            {
                "name": "churn_model_v1",
                "inputs": ["age", "tenure"],    # optional; defaults to all columns
                "outputs": ["churn_score"],      # optional for DataFrame results
                "params": {"region": "eu-west-1"},  # optional kwargs
            }

        The registered function is expected to accept a DataFrame (projected
        to the specified input columns) and keyword arguments, and return
        either a Series or DataFrame whose index aligns with ``df``.
        """
        name = spec["name"]
        if name not in self.pipeline_functions:
            raise ValueError(f"Unknown pipeline function '{name}' in metadata")

        func = self.pipeline_functions[name]
        inputs = spec.get("inputs")
        outputs = spec.get("outputs")
        params = spec.get("params", {})

        input_df = df if inputs is None else df[inputs]
        result = func(input_df, **params)

        if isinstance(result, pd.Series):
            if not outputs or len(outputs) != 1:
                raise ValueError(
                    f"Pipeline function '{name}' returned a Series but "
                    "metadata did not specify exactly one output column"
                )
            col_name = outputs[0]
            return df.assign(**{col_name: result})

        if isinstance(result, pd.DataFrame):
            if outputs is not None:
                if len(outputs) != result.shape[1]:
                    raise ValueError(
                        f"Pipeline function '{name}' returned {result.shape[1]} "
                        f"columns but metadata specifies {len(outputs)} outputs"
                    )
                result = result.set_axis(outputs, axis=1)
            return df.join(result)

        raise TypeError(
            f"Pipeline function '{name}' must return a Series or DataFrame, "
            f"got {type(result)!r}"
        )

    def _apply_lookup_operation(self, df: pd.DataFrame, spec: Dict[str, Any]) -> pd.Series:
        """Apply a lookup operation described by metadata.

        The spec supports the following keys::

            {
                "resolver": "prices",      # name of registered resolver (preferred)
                # or
                "mapping": {"a": 1, "b": 2},  # inline mapping for small cases
                "key": "product",             # column providing lookup keys
                "on_missing": "null",         # "null" | "keep" | "raise"
            }
        """
        key_col = spec["key"]
        on_missing = spec.get("on_missing", "null")

        if "resolver" in spec:
            resolver_name = spec["resolver"]
            try:
                resolver = self.resolvers[resolver_name]
            except KeyError as exc:
                raise ValueError(
                    f"Unknown resolver '{resolver_name}' in lookup metadata"
                ) from exc
        elif "mapping" in spec:
            from df_eval.lookup import DictResolver

            resolver = DictResolver(spec["mapping"])
        else:
            raise ValueError("lookup metadata requires either 'resolver' or 'mapping'")

        return _lookup(df[key_col], resolver, on_missing=on_missing)

    def apply_pandera_schema(
        self,
        df: pd.DataFrame,
        schema: Any,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Apply a Pandera schema and derive df-eval columns from metadata.

        This is a thin convenience wrapper around
        ``df_eval.pandera.apply_pandera_schema`` that forwards the current
        engine instance so registered functions/constants and provenance
        settings are honored.
        """
        from df_eval.pandera import apply_pandera_schema

        return apply_pandera_schema(df, schema, engine=self, **kwargs)

    def iter_apply_schema_parquet_chunks(
        self,
        input_path: str | Path,
        schema: Dict[str, str | Expression],
        *,
        dtypes: Optional[Dict[str, str]] = None,
        chunk_size: int = 100_000,
        input_columns: Sequence[str] | None = None,
        output_columns: Sequence[str] | None = None,
    ) -> Iterator[pd.DataFrame]:
        """Yield transformed chunks from a Parquet file or dataset.

        Args:
            input_path: Source Parquet file or directory-backed dataset.
            schema: Mapping of derived column names to expressions.
            dtypes: Optional mapping of derived column names to pandas dtypes.
            chunk_size: Maximum rows to scan and transform per chunk.
            input_columns: Optional input column projection for scan efficiency.
            output_columns: Optional ordered subset of output columns to keep.

        Yields:
            Transformed DataFrame chunks.
        """
        selected_output_columns = list(output_columns) if output_columns is not None else None

        for chunk in iter_parquet_row_chunks(
            input_path,
            chunk_size=chunk_size,
            columns=input_columns,
        ):
            transformed = self.apply_schema(chunk, schema, dtypes=dtypes)
            if selected_output_columns is not None:
                transformed = transformed.loc[:, selected_output_columns]
            yield transformed

    def apply_schema_parquet_to_df(
        self,
        input_path: str | Path,
        schema: Dict[str, str | Expression],
        *,
        dtypes: Optional[Dict[str, str]] = None,
        chunk_size: int = 100_000,
        input_columns: Sequence[str] | None = None,
        output_columns: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Transform a Parquet dataset chunk-by-chunk and return one DataFrame.

        Args:
            input_path: Source Parquet file or directory-backed dataset.
            schema: Mapping of derived column names to expressions.
            dtypes: Optional mapping of derived column names to pandas dtypes.
            chunk_size: Maximum rows to process per chunk.
            input_columns: Optional input column projection for scan efficiency.
            output_columns: Optional ordered subset of output columns to keep.

        Returns:
            A DataFrame containing all transformed rows. Returns an empty
            DataFrame when the input yields no row chunks.
        """
        chunks = list(
            self.iter_apply_schema_parquet_chunks(
                input_path,
                schema,
                dtypes=dtypes,
                chunk_size=chunk_size,
                input_columns=input_columns,
                output_columns=output_columns,
            )
        )
        if not chunks:
            return pd.DataFrame()
        return pd.concat(chunks, ignore_index=True)

    def apply_schema_parquet_to_parquet(
        self,
        input_path: str | Path,
        output_path: str | Path,
        schema: Dict[str, str | Expression],
        *,
        dtypes: Optional[Dict[str, str]] = None,
        chunk_size: int = 100_000,
        input_columns: Sequence[str] | None = None,
        output_columns: Sequence[str] | None = None,
        compression: str = "snappy",
    ) -> Path:
        """Transform a Parquet dataset chunk-by-chunk and write Parquet output.

        This method is optimized for out-of-memory processing: source data is
        streamed in row chunks, transformed with the same expression engine
        used for in-memory DataFrames, and written incrementally to ``output_path``.

        Args:
            input_path: Source Parquet file or directory-backed dataset.
            output_path: Destination Parquet file.
            schema: Mapping of derived column names to expressions.
            dtypes: Optional mapping of derived column names to pandas dtypes.
            chunk_size: Maximum rows to process per chunk.
            input_columns: Optional input column projection for scan efficiency.
            output_columns: Optional ordered subset of output columns to keep.
            compression: Parquet compression codec used for output.

        Returns:
            The normalized ``output_path``.
        """
        transformed_chunks = self.iter_apply_schema_parquet_chunks(
            input_path,
            schema,
            dtypes=dtypes,
            chunk_size=chunk_size,
            input_columns=input_columns,
            output_columns=output_columns,
        )
        return write_parquet_row_chunks(
            transformed_chunks,
            output_path,
            compression=compression,
        )

    def apply_pandera_schema_parquet_to_parquet(
        self,
        input_path: str | Path,
        output_path: str | Path,
        schema: Any,
        **kwargs: Any,
    ) -> Path:
        """Apply a Pandera schema to Parquet input and write Parquet output."""
        from df_eval.pandera import apply_pandera_schema_parquet_to_parquet

        return apply_pandera_schema_parquet_to_parquet(
            input_path,
            output_path,
            schema,
            engine=self,
            **kwargs,
        )

    def _topological_sort(
        self,
        expressions: Dict[str, Expression],
        existing_cols: Set[str]
    ) -> List[str]:
        """
        Perform topological sort on expressions based on dependencies.
        
        Args:
            expressions: Dictionary of column names to Expression objects.
            existing_cols: Set of existing column names in the DataFrame.
            
        Returns:
            List of column names in dependency order.
            
        Raises:
            CycleDetectedError: If a cycle is detected.
        """
        # Build dependency graph
        # graph[A] = {B, C} means A depends on B and C (B and C must be evaluated first)
        graph: Dict[str, Set[str]] = {}
        in_degree: Dict[str, int] = {}
        
        # Initialize all nodes with zero in-degree
        for col_name in expressions:
            in_degree[col_name] = 0
            graph[col_name] = set()
        
        # Build graph: for each column, record what it depends on
        for col_name, expr in expressions.items():
            # Only consider dependencies on other derived columns
            deps = expr.dependencies & expressions.keys()
            graph[col_name] = deps
            # This column has incoming edges from each dependency
            in_degree[col_name] = len(deps)
        
        # Kahn's algorithm for topological sort
        # Start with nodes that have no dependencies (in-degree = 0)
        queue = [col for col, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            # Sort for deterministic output
            queue.sort()
            node = queue.pop(0)
            result.append(node)
            
            # This node is evaluated, so check all other nodes
            # If any depend on this node, reduce their in-degree
            for other_col in expressions.keys():
                if node in graph[other_col]:
                    in_degree[other_col] -= 1
                    if in_degree[other_col] == 0:
                        queue.append(other_col)
        
        # Check for cycles
        if len(result) != len(expressions):
            remaining = set(expressions.keys()) - set(result)
            raise CycleDetectedError(
                f"Cycle detected in column dependencies: {remaining}"
            )
        
        return result
