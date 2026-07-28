Lookups
=======

This guide covers df-eval's lookup functionality for integrating external data sources.

Overview
--------

Lookups allow you to resolve values from external data sources during expression evaluation. This is useful for:

- Mapping codes to descriptions
- Looking up prices or rates
- Resolving configuration values
- Fetching data from databases or APIs
- Implementing business rules from external sources

Basic Lookup Usage
------------------

The ``lookup`` function resolves values using a resolver:

.. code-block:: python

   import pandas as pd
   from df_eval import lookup, DictResolver

   mapping = {
       "apple": 1.50,
       "banana": 0.75,
       "orange": 1.25,
   }

   resolver = DictResolver(mapping, default=0.0)

   products = pd.Series(["apple", "banana", "cherry"])
   prices = lookup(products, resolver, on_missing="null")
   print(prices)  # [1.50, 0.75, 0.0]

For a complete, runnable walkthrough that combines lookups with
expression evaluation, see the example
:ref:`sphx_glr_auto_examples_lookup_engine_integration.py`.

Resolver Types
--------------

DictResolver
^^^^^^^^^^^^

The simplest resolver for in-memory mappings:

.. code-block:: python

   from df_eval import DictResolver

   mapping = {
       "USD": 1.0,
       "EUR": 0.85,
       "GBP": 0.73,
       "JPY": 110.0,
   }

   resolver = DictResolver(mapping, default=1.0)

   currencies = pd.Series(["USD", "EUR", "GBP", "CAD"])
   rates = lookup(currencies, resolver, on_missing="keep")
   print(rates)

FileResolver
^^^^^^^^^^^^

Load mappings from CSV or JSON files:

.. code-block:: python

   from df_eval import FileResolver

   resolver = FileResolver(
       "prices.csv",
       key_column="product",
       value_column="price",
   )

   products = pd.Series(["apple", "banana", "cherry"])
   prices = lookup(products, resolver, on_missing="null")

Custom Resolver Classes
^^^^^^^^^^^^^^^^^^^^^^^

Create custom resolvers by inheriting from ``Resolver`` and implementing
``resolve(self, key)``:

.. code-block:: python

   from df_eval import Resolver

   class DatabaseResolver(Resolver):
       def __init__(self, connection, table, key_col, value_col):
           self.connection = connection
           self.table = table
           self.key_col = key_col
           self.value_col = value_col

       def resolve(self, key):
           query = f"""
               SELECT {self.value_col}
               FROM {self.table}
               WHERE {self.key_col} = ?
           """
           result = pd.read_sql(query, self.connection, params=[key])
           if len(result) == 0:
               return None
           return result[self.value_col].iloc[0]

Caching
-------

Improve performance by caching lookup results:

.. code-block:: python

   from df_eval import CachedResolver, FileResolver, lookup

   base_resolver = FileResolver("prices.csv", "product", "price")
   cached_resolver = CachedResolver(base_resolver, ttl_seconds=300)

   prices1 = lookup(products, cached_resolver)
   prices2 = lookup(products, cached_resolver)

Manual Cache Management
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   cached_resolver.clear_cache()

Missing Value Handling
----------------------

Control how missing values are handled via ``on_missing``:

.. code-block:: python

   # Return None for missing values
   result = lookup(keys, resolver, on_missing="null")

   # Raise error on missing values
   try:
       result = lookup(keys, resolver, on_missing="raise")
   except ValueError as e:
       print(f"Missing keys: {e}")

   # Return the original key for missing values
   result = lookup(keys, resolver, on_missing="keep")

Using Lookups in Pipelines
--------------------------

Integrate lookups into df-eval pipelines by combining resolvers with regular
expressions. Lookups themselves are performed in Python using the helper
function :func:`df_eval.lookup.lookup` or via Pandera metadata.

Basic engine integration:

.. code-block:: python

   import pandas as pd
   from df_eval import Engine, DictResolver, lookup

   price_resolver = DictResolver({
       "apple": 1.50,
       "banana": 0.75,
       "orange": 1.25,
   })

   engine = Engine()
   engine.register_resolver("prices", price_resolver)

   df = pd.DataFrame({
       "product": ["apple", "banana", "orange"],
       "quantity": [10, 20, 15],
   })

   prices = lookup(df["product"], price_resolver, on_missing="null")
   df = df.assign(price=prices)

   schema = {
       "total": "price * quantity",
   }

   result = engine.apply_schema(df, schema)

See also the gallery example
:ref:`sphx_glr_auto_examples_lookup_engine_integration.py` for a
slightly more complete pipeline using in-memory resolvers, and
:ref:`sphx_glr_auto_examples_lookup_pandera_pipeline.py` for an
end-to-end pipeline that stores lookup configuration in a Pandera
schema.

Error Handling
--------------

Handle lookup errors gracefully:

.. code-block:: python

   from df_eval import lookup

   try:
       result = lookup(keys, resolver, on_missing="raise")
   except ValueError as e:
       print(f"Lookup failed: {e}")
       result = lookup(keys, resolver, on_missing="null")

Best Practices
--------------

1. Use Caching for Expensive Lookups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   expensive_resolver = DatabaseResolver(...)
   cached = CachedResolver(expensive_resolver, ttl_seconds=300)

2. Keep Resolver Logic Fast
^^^^^^^^^^^^^^^^^^^^^^^^^^^

``lookup`` calls ``resolve`` once per key in the input series. Prefer
resolvers that avoid repeated expensive network or database calls.

3. Handle Missing Values Explicitly
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   result = lookup(keys, resolver, on_missing="keep")

Next Steps
----------

- Check the :doc:`../reference/api` for complete documentation
- Review :doc:`basic_usage` for core concepts
- Explore :doc:`advanced_usage` for more features
