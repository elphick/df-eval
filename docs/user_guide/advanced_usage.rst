Advanced Usage
==============

This guide covers advanced features of df-eval including dependency management, provenance tracking, and custom functions.

Dependency Management
---------------------

Automatic Topological Sorting
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When you define derived columns that depend on other derived columns, df-eval automatically determines the correct evaluation order:

.. code-block:: python

   import pandas as pd
   from df_eval import Engine

   df = pd.DataFrame({
       "a": [1, 2, 3]
   })

   engine = Engine()

   # Define columns with dependencies
   # The engine automatically determines the correct evaluation order
   schema = {
       "b": "a * 2",       # Depends on a
       "c": "b + 10",      # Depends on b
       "d": "c * a"        # Depends on both c and a
   }

   result = engine.apply_schema(df, schema)
   # Columns are evaluated in the correct order: b, c, d
   print(result)
   #    a   b   c   d
   # 0  1   2  12  12
   # 1  2   4  14  28
   # 2  3   6  16  48

The engine builds a dependency graph and uses topological sorting to ensure columns are computed in the right order.

Cycle Detection
^^^^^^^^^^^^^^^

df-eval automatically detects circular dependencies and raises an error:

.. code-block:: python

   from df_eval.exceptions import CycleDetectedError

   # This schema has a circular dependency
   schema = {
       "x": "y + 1",  # x depends on y
       "y": "x + 1"   # y depends on x - creates a cycle!
   }

   try:
       result = engine.apply_schema(df, schema)
   except CycleDetectedError as e:
       print(f"Cycle detected: {e}")
       # Output: Cycle detected: Circular dependency: x -> y -> x

Complex Dependency Graphs
^^^^^^^^^^^^^^^^^^^^^^^^^^

You can create complex dependency graphs with multiple levels:

.. code-block:: python

   schema = {
       "step1_a": "a * 2",
       "step1_b": "a + 5",
       "step2": "step1_a + step1_b",
       "step3_a": "step2 * 0.5",
       "step3_b": "step2 - step1_a",
       "final": "step3_a + step3_b"
   }

   result = engine.apply_schema(df, schema)

The engine handles all the complexity of determining evaluation order.

Provenance Tracking
-------------------

Track the origin and dependencies of derived columns:

.. code-block:: python

   # Enable provenance tracking
   engine.enable_provenance(True)

   schema = {
       "derived1": "a + b",
       "derived2": "derived1 * 2"
   }

   df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
   result = engine.apply_schema(df, schema)

   # Access provenance information
   provenance = result.attrs.get('df_eval_provenance', {})
   print(provenance)
   # {
   #   'derived1': {
   #     'expression': 'a + b',
   #     'dependencies': ['a', 'b']
   #   },
   #   'derived2': {
   #     'expression': 'derived1 * 2',
   #     'dependencies': ['derived1']
   #   }
   # }

Provenance information includes:

- The original expression used to create the column
- Direct dependencies (columns referenced in the expression)
- Metadata about when the column was created

Using Provenance for Debugging
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Provenance tracking is useful for:

- Understanding data lineage
- Debugging complex transformations
- Auditing data transformations
- Generating documentation

.. code-block:: python

   # Print lineage for a specific column
   def print_lineage(provenance, column_name, level=0):
       if column_name not in provenance:
           return
       
       info = provenance[column_name]
       indent = "  " * level
       print(f"{indent}{column_name} = {info['expression']}")
       
       for dep in info['dependencies']:
           print_lineage(provenance, dep, level + 1)

   print_lineage(provenance, "derived2")

Custom Functions (UDFs)
-----------------------

Register custom functions to extend df-eval's capabilities:

Basic UDF Registration
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   import numpy as np

   # Define a custom function
   def custom_transform(x):
       """Square the input and add 10."""
       return x ** 2 + 10

   # Register the function
   engine.register_function("transform", custom_transform)

   # Use it in expressions
   df = pd.DataFrame({"a": [1, 2, 3]})
   result = engine.evaluate(df, "transform(a)")
   print(result)  # [11, 14, 19]

Multi-Argument UDFs
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   def weighted_sum(x, y, weight_x=0.5, weight_y=0.5):
       """Calculate weighted sum of two series."""
       return x * weight_x + y * weight_y

   engine.register_function("weighted_sum", weighted_sum)

   # Use with positional arguments
   result = engine.evaluate(df, "weighted_sum(a, b, 0.7, 0.3)")

   # Use with keyword arguments
   result = engine.evaluate(df, "weighted_sum(a, b, weight_x=0.8, weight_y=0.2)")

Vectorized UDFs
^^^^^^^^^^^^^^^

For best performance, use vectorized operations:

.. code-block:: python

   def vectorized_clip_scale(x, min_val, max_val, scale):
       """Clip and scale in one operation."""
       clipped = np.clip(x, min_val, max_val)
       return clipped * scale

   engine.register_function("clip_scale", vectorized_clip_scale)

   result = engine.evaluate(df, "clip_scale(a, 0, 100, 0.1)")

Custom Constants
----------------

Register constants for use in expressions:

.. code-block:: python

   # Register mathematical constants
   engine.register_constant("PI", 3.14159265359)
   engine.register_constant("E", 2.71828182846)

   # Register domain-specific constants
   engine.register_constant("TAX_RATE", 0.07)
   engine.register_constant("DISCOUNT_RATE", 0.15)
   engine.register_constant("THRESHOLD", 1000)

   # Use in expressions
   df = pd.DataFrame({
       "radius": [1, 2, 3],
       "price": [100, 200, 300]
   })

   schema = {
       "area": "PI * radius ** 2",
       "price_with_tax": "price * (1 + TAX_RATE)",
       "discounted_price": "where(price > THRESHOLD, price * (1 - DISCOUNT_RATE), price)"
   }

   result = engine.apply_schema(df, schema)

Out-of-Memory Parquet Workflows
-------------------------------

For large datasets, you can keep memory usage bounded by streaming rows from
Parquet input, applying schema logic chunk-by-chunk, and writing Parquet output.

.. code-block:: python

   from df_eval import Engine

   engine = Engine()
   schema = {
       "line_total": "price * qty",
       "line_total_with_fee": "line_total + 1.5",
   }

   # Fully out-of-memory: parquet in, parquet out
   engine.apply_schema_parquet_to_parquet(
       "input.parquet",
       "output.parquet",
       schema,
       chunk_size=100_000,
       input_columns=["price", "qty"],
   )

If you want to inspect the transformed result in-memory, use:

.. code-block:: python

   transformed_df = engine.apply_schema_parquet_to_df(
       "input.parquet",
       schema,
       chunk_size=100_000,
       input_columns=["price", "qty"],
   )

For a complete runnable walkthrough, see
:doc:`../auto_examples/parquet_out_of_memory`.

Engine Configuration
--------------------

Creating Multiple Engines
^^^^^^^^^^^^^^^^^^^^^^^^^^

You can create multiple engines with different configurations:

.. code-block:: python

   # Engine for financial calculations
   financial_engine = Engine()
   financial_engine.register_constant("TAX_RATE", 0.07)
   financial_engine.register_constant("INTEREST_RATE", 0.05)

   # Engine for scientific calculations
   science_engine = Engine()
   science_engine.register_constant("G", 9.81)  # gravity
   science_engine.register_constant("C", 299792458)  # speed of light

Copying Engines
^^^^^^^^^^^^^^^

Create a copy of an engine with all its registered functions and constants:

.. code-block:: python

   # Create base engine with common functions
   base_engine = Engine()
   base_engine.register_function("custom_func", my_func)
   base_engine.register_constant("CONSTANT", 42)

   # Create specialized engines from base
   engine1 = base_engine.copy()
   engine1.register_constant("SPECIFIC_PARAM", 1.5)

   engine2 = base_engine.copy()
   engine2.register_constant("SPECIFIC_PARAM", 2.5)

Performance Optimization
------------------------

Pandera Integration
-------------------

If you use Pandera as your schema source of truth, you can attach df-eval
expressions to per-column metadata and evaluate them through the engine.

The convention is:

- metadata key: ``metadata["df-eval"]``
- expression key: ``metadata["df-eval"]["expr"]``

Engine-first entry point
^^^^^^^^^^^^^^^^^^^^^^^^

Use ``Engine.apply_pandera_schema`` for discoverability and a centralized
workflow:

.. code-block:: python

   import pandas as pd
   import pandera.pandas as pa
   from df_eval import Engine

   schema = pa.DataFrameSchema(
       {
           "value": pa.Column(float, coerce=True),
           "weight": pa.Column(float, coerce=True),
           "weighted": pa.Column(
               float,
               coerce=True,
               metadata={"df-eval": {"expr": "value * weight"}},
           ),
       }
   )

   df = pd.DataFrame({"value": ["10", "20"], "weight": ["0.5", "0.25"]})

   engine = Engine()
   result = engine.apply_pandera_schema(
       df,
       schema,
       coerce=True,
       validate=True,
       validate_post=True,
   )

   # result includes derived column "weighted"

Equivalent functional helper
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you prefer a functional style, use ``df_eval.pandera.apply_pandera_schema``:

.. code-block:: python

   from df_eval.pandera import apply_pandera_schema

   result = apply_pandera_schema(df, schema, coerce=True, validate=True, validate_post=True)

Batch Operations
----------------

Process multiple DataFrames with the same schema efficiently:

.. code-block:: python

   schema = {
       "derived": "a * 2 + b"
   }

   # Process multiple DataFrames
   dataframes = [df1, df2, df3]
   results = [engine.apply_schema(df, schema) for df in dataframes]

Reusing Compiled Expressions
----------------------------

The engine caches compiled expressions for better performance:

.. code-block:: python

   # First evaluation compiles the expression
   result1 = engine.evaluate(df1, "a + b * 2")

   # Subsequent evaluations with the same expression are faster
   result2 = engine.evaluate(df2, "a + b * 2")
   result3 = engine.evaluate(df3, "a + b * 2")

Next Steps
----------

- Learn about :doc:`lookups` for integrating external data sources
- Check the :doc:`../reference/api` for complete documentation
