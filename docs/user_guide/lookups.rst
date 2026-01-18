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

   # Create a mapping
   mapping = {
       "apple": 1.50,
       "banana": 0.75,
       "orange": 1.25
   }

   # Create a resolver
   resolver = DictResolver(mapping, default=0.0)

   # Lookup values
   products = pd.Series(["apple", "banana", "cherry"])
   prices = lookup(products, resolver, on_missing="null")
   print(prices)  # [1.50, 0.75, None]

Resolver Types
--------------

DictResolver
^^^^^^^^^^^^

The simplest resolver for in-memory mappings:

.. code-block:: python

   from df_eval import DictResolver

   # Basic dictionary resolver
   mapping = {
       "USD": 1.0,
       "EUR": 0.85,
       "GBP": 0.73,
       "JPY": 110.0
   }

   resolver = DictResolver(mapping, default=1.0)

   currencies = pd.Series(["USD", "EUR", "GBP", "CAD"])
   rates = lookup(currencies, resolver, on_missing="default")
   print(rates)  # [1.0, 0.85, 0.73, 1.0]

FileResolver
^^^^^^^^^^^^

Load mappings from CSV, JSON, or Excel files:

.. code-block:: python

   from df_eval import FileResolver

   # Lookup from CSV file
   # prices.csv:
   #   product,price
   #   apple,1.50
   #   banana,0.75
   #   orange,1.25

   resolver = FileResolver(
       "prices.csv",
       key_column="product",
       value_column="price"
   )

   products = pd.Series(["apple", "banana", "cherry"])
   prices = lookup(products, resolver, on_missing="null")

Database Resolvers
^^^^^^^^^^^^^^^^^^

Create custom resolvers for database lookups:

.. code-block:: python

   from df_eval import BaseResolver

   class DatabaseResolver(BaseResolver):
       def __init__(self, connection_string, table, key_col, value_col):
           self.conn = create_connection(connection_string)
           self.table = table
           self.key_col = key_col
           self.value_col = value_col
       
       def resolve(self, keys):
           """Resolve keys to values using database query."""
           # Build query with parameterized values
           placeholders = ','.join(['?'] * len(keys))
           query = f"""
               SELECT {self.key_col}, {self.value_col}
               FROM {self.table}
               WHERE {self.key_col} IN ({placeholders})
           """
           
           # Execute query
           results = pd.read_sql(query, self.conn, params=keys.tolist())
           
           # Map results back to keys
           mapping = dict(zip(results[self.key_col], results[self.value_col]))
           return keys.map(mapping)

   # Use the custom resolver
   resolver = DatabaseResolver(
       "postgresql://localhost/mydb",
       table="products",
       key_col="product_id",
       value_col="product_name"
   )

   ids = pd.Series([101, 102, 103])
   names = lookup(ids, resolver)

HTTP API Resolvers
^^^^^^^^^^^^^^^^^^

Fetch data from web APIs:

.. code-block:: python

   import requests

   class APIResolver(BaseResolver):
       def __init__(self, api_url, api_key=None):
           self.api_url = api_url
           self.api_key = api_key
       
       def resolve(self, keys):
           """Resolve keys using HTTP API."""
           headers = {}
           if self.api_key:
               headers['Authorization'] = f'Bearer {self.api_key}'
           
           # Batch request
           response = requests.post(
               self.api_url,
               json={'keys': keys.tolist()},
               headers=headers
           )
           response.raise_for_status()
           
           # Parse response
           data = response.json()
           mapping = {item['key']: item['value'] for item in data['results']}
           return keys.map(mapping)

   # Use the API resolver
   resolver = APIResolver(
       "https://api.example.com/lookup",
       api_key="your-api-key"
   )

   codes = pd.Series(["ABC", "DEF", "GHI"])
   values = lookup(codes, resolver)

Caching
-------

Improve performance by caching lookup results:

.. code-block:: python

   from df_eval import CachedResolver

   # Wrap any resolver with caching
   base_resolver = FileResolver("prices.csv", "product", "price")
   cached_resolver = CachedResolver(base_resolver, ttl_seconds=300)

   # First lookup hits the file
   prices1 = lookup(products, cached_resolver)

   # Second lookup uses cache (faster)
   prices2 = lookup(products, cached_resolver)

   # Cache expires after 5 minutes (300 seconds)

Cache Configuration
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Configure cache behavior
   cached_resolver = CachedResolver(
       base_resolver,
       ttl_seconds=600,        # Cache for 10 minutes
       max_size=10000,         # Store up to 10,000 entries
       refresh_on_hit=True     # Reset TTL when cache is hit
   )

Manual Cache Management
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Clear the cache
   cached_resolver.clear_cache()

   # Check cache statistics
   stats = cached_resolver.get_stats()
   print(f"Cache hits: {stats['hits']}")
   print(f"Cache misses: {stats['misses']}")
   print(f"Cache size: {stats['size']}")

Missing Value Handling
----------------------

Control how missing values are handled:

.. code-block:: python

   # Return None for missing values
   result = lookup(keys, resolver, on_missing="null")

   # Use default value from resolver
   result = lookup(keys, resolver, on_missing="default")

   # Raise error on missing values
   try:
       result = lookup(keys, resolver, on_missing="raise")
   except KeyError as e:
       print(f"Missing keys: {e}")

   # Return the original key for missing values
   result = lookup(keys, resolver, on_missing="key")

Using Lookups in Expressions
-----------------------------

Integrate lookups into schema definitions:

.. code-block:: python

   from df_eval import Engine

   # Setup resolver
   price_resolver = DictResolver({
       "apple": 1.50,
       "banana": 0.75,
       "orange": 1.25
   })

   # Register resolver with engine
   engine = Engine()
   engine.register_resolver("prices", price_resolver)

   # Use in expressions
   df = pd.DataFrame({
       "product": ["apple", "banana", "orange"],
       "quantity": [10, 20, 15]
   })

   schema = {
       "price": "lookup(product, prices)",
       "total": "price * quantity"
   }

   result = engine.apply_schema(df, schema)
   print(result)
   #   product  quantity  price  total
   # 0   apple        10   1.50  15.00
   # 1  banana        20   0.75  15.00
   # 2  orange        15   1.25  18.75

Chaining Lookups
----------------

Perform multiple lookups in sequence:

.. code-block:: python

   # Setup multiple resolvers
   category_resolver = DictResolver({
       "apple": "fruit",
       "banana": "fruit",
       "carrot": "vegetable"
   })

   tax_rate_resolver = DictResolver({
       "fruit": 0.05,
       "vegetable": 0.03,
       "other": 0.08
   })

   engine.register_resolver("categories", category_resolver)
   engine.register_resolver("tax_rates", tax_rate_resolver)

   # Chain lookups
   schema = {
       "category": "lookup(product, categories)",
       "tax_rate": "lookup(category, tax_rates)",
       "price_with_tax": "price * (1 + tax_rate)"
   }

   result = engine.apply_schema(df, schema)

Batch Lookups
-------------

Optimize lookups for large datasets:

.. code-block:: python

   # Resolvers should implement batch resolution
   class BatchResolver(BaseResolver):
       def resolve(self, keys):
           """Resolve all keys in a single batch operation."""
           # Get unique keys to minimize lookups
           unique_keys = keys.unique()
           
           # Fetch all values at once (e.g., single database query)
           mapping = self._batch_fetch(unique_keys)
           
           # Map back to original keys
           return keys.map(mapping)
       
       def _batch_fetch(self, keys):
           # Implement efficient batch fetching
           pass

Error Handling
--------------

Handle lookup errors gracefully:

.. code-block:: python

   from df_eval import lookup, LookupError

   try:
       result = lookup(keys, resolver, on_missing="raise")
   except LookupError as e:
       print(f"Lookup failed: {e}")
       print(f"Missing keys: {e.missing_keys}")
       
       # Fallback strategy
       result = lookup(keys, resolver, on_missing="default")

Best Practices
--------------

1. Use Caching for Expensive Lookups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Always wrap expensive resolvers (DB, API) with caching
   expensive_resolver = DatabaseResolver(...)
   cached = CachedResolver(expensive_resolver, ttl_seconds=300)

2. Batch Operations
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Implement batch resolution for better performance
   def resolve(self, keys):
       # Fetch all keys at once, not one-by-one
       unique_keys = keys.unique()
       mapping = self._fetch_batch(unique_keys)
       return keys.map(mapping)

3. Handle Missing Values
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Always specify on_missing behavior
   result = lookup(keys, resolver, on_missing="default")

4. Monitor Cache Performance
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Check cache hit rate periodically
   stats = cached_resolver.get_stats()
   hit_rate = stats['hits'] / (stats['hits'] + stats['misses'])
   if hit_rate < 0.8:
       print("Consider increasing cache size or TTL")

Next Steps
----------

- Check the :doc:`../reference/api` for complete documentation
- Review :doc:`basic_usage` for core concepts
- Explore :doc:`advanced_usage` for more features
