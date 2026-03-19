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

Pandera Integration and Schema IO
---------------------------------

df-eval integrates with `Pandera <https://pandera.readthedocs.io/>`_ to drive
schema-based derived columns from Pandera ``DataFrameSchema`` objects or
SchemaModel/DataFrameModel classes.

The :mod:`df_eval.pandera` module understands per-column metadata and can
translate it into df-eval operations. For example, you can attach a df-eval
expression directly to a Pandera column:

.. code-block:: python

   import pandas as pd
   import pandera as pa

   from df_eval import Engine
   from df_eval.pandera import apply_pandera_schema

   schema = pa.DataFrameSchema(
       {
           "a": pa.Column(int),
           "b": pa.Column(int),
           "sum": pa.Column(
               int,
               metadata={"df-eval": {"expr": "a + b"}},
           ),
       }
   )

   df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})

   engine = Engine()
   result = apply_pandera_schema(df, schema)
   print(result["sum"].tolist())  # [4, 6]

Pandera Schema IO with Metadata Preservation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Pandera currently has an open issue (:issue:`https://github.com/unionai-oss/pandera/issues/1301`)
where column ``metadata`` is not preserved when using its built-in YAML/JSON
IO helpers. This is important for df-eval because column metadata is where
we store df-eval expressions and other operation specs.

To work around this, df-eval ships a small compatibility layer that mirrors
``pandera.io.pandas_io`` but ensures that column ``metadata`` is included in
schema statistics and survives IO round-trips. You typically don't need to
import this compat layer directly; instead, use the public helpers in
:mod:`df_eval.pandera`:

.. code-block:: python

   from pathlib import Path

   import pandera as pa
   from df_eval.pandera import (
       load_pandera_schema_yaml,
       dump_pandera_schema_yaml,
       df_eval_schema_from_pandera,
   )

   # Define a schema with df-eval metadata
   schema = pa.DataFrameSchema(
       {
           "value": pa.Column(float),
           "double": pa.Column(
               float,
               metadata={"df-eval": {"expr": "2 * value"}},
           ),
       }
   )

   # Round-trip via YAML while preserving metadata
   yaml_text = dump_pandera_schema_yaml(schema)
   loaded = load_pandera_schema_yaml(yaml_text)

   # df-eval expressions are preserved under metadata["df-eval"]["expr"]
   expr_map = df_eval_schema_from_pandera(loaded)
   assert expr_map == {"double": "2 * value"}

The following helpers are available:

* :func:`df_eval.pandera.load_pandera_schema_yaml` – load a Pandera
  ``DataFrameSchema`` from YAML (path or string) with metadata preserved.
* :func:`df_eval.pandera.dump_pandera_schema_yaml` – dump a Pandera
  ``DataFrameSchema`` to YAML, including column metadata.
* :func:`df_eval.pandera.load_pandera_schema_json` – load a schema from JSON.
* :func:`df_eval.pandera.dump_pandera_schema_json` – dump a schema to JSON.

These helpers require the optional Pandera IO dependencies; install them via:

.. code-block:: bash

   pip install "df-eval[pandera]"
