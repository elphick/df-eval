"""Tests for lookup functionality and resolvers."""

import pytest
import pandas as pd
import numpy as np
from df_eval.lookup import (
    lookup, Resolver, CachedResolver, DictResolver, 
    FileResolver, DatabaseResolver, HTTPResolver
)
import tempfile
import os


def test_dict_resolver():
    """Test DictResolver."""
    mapping = {"a": 1, "b": 2, "c": 3}
    resolver = DictResolver(mapping, default=0)
    
    assert resolver.resolve("a") == 1
    assert resolver.resolve("b") == 2
    assert resolver.resolve("x") == 0  # Default


def test_cached_resolver():
    """Test CachedResolver with TTL."""
    # Create a mock resolver that counts calls
    call_count = {"count": 0}
    
    class CountingResolver(Resolver):
        def resolve(self, key):
            call_count["count"] += 1
            return key * 2
    
    base_resolver = CountingResolver()
    cached_resolver = CachedResolver(base_resolver, ttl_seconds=1.0)
    
    # First call should hit the resolver
    result1 = cached_resolver.resolve(5)
    assert result1 == 10
    assert call_count["count"] == 1
    
    # Second call should use cache
    result2 = cached_resolver.resolve(5)
    assert result2 == 10
    assert call_count["count"] == 1  # No additional call
    
    # Clear cache and verify
    cached_resolver.clear_cache()
    result3 = cached_resolver.resolve(5)
    assert result3 == 10
    assert call_count["count"] == 2  # Called again


def test_file_resolver_csv():
    """Test FileResolver with CSV file."""
    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("key,value\n")
        f.write("a,10\n")
        f.write("b,20\n")
        f.write("c,30\n")
        temp_file = f.name
    
    try:
        resolver = FileResolver(temp_file, "key", "value")
        assert resolver.resolve("a") == 10
        assert resolver.resolve("b") == 20
        assert resolver.resolve("x") is None
    finally:
        os.unlink(temp_file)


def test_file_resolver_json():
    """Test FileResolver with JSON file."""
    # Create a temporary JSON file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write('[{"key": "a", "value": 100}, {"key": "b", "value": 200}]')
        temp_file = f.name
    
    try:
        resolver = FileResolver(temp_file, "key", "value")
        assert resolver.resolve("a") == 100
        assert resolver.resolve("b") == 200
    finally:
        os.unlink(temp_file)


def test_lookup_with_null_on_missing():
    """Test lookup function with null on missing."""
    series = pd.Series(["a", "b", "c", "x"])
    mapping = {"a": 1, "b": 2, "c": 3}
    resolver = DictResolver(mapping)
    
    result = lookup(series, resolver, on_missing="null")
    expected = pd.Series([1, 2, 3, None])
    pd.testing.assert_series_equal(result, expected)


def test_lookup_with_keep_on_missing():
    """Test lookup function with keep on missing."""
    series = pd.Series(["a", "b", "x"])
    mapping = {"a": 10, "b": 20}
    resolver = DictResolver(mapping)
    
    result = lookup(series, resolver, on_missing="keep")
    expected = pd.Series([10, 20, "x"])
    pd.testing.assert_series_equal(result, expected)


def test_lookup_with_raise_on_missing():
    """Test lookup function with raise on missing."""
    series = pd.Series(["a", "b", "x"])
    mapping = {"a": 1, "b": 2}
    resolver = DictResolver(mapping)
    
    with pytest.raises(ValueError, match="Could not resolve key: x"):
        lookup(series, resolver, on_missing="raise")


def test_database_resolver_not_implemented():
    """Test that DatabaseResolver raises NotImplementedError."""
    resolver = DatabaseResolver("connection_string", "table", "key", "value")
    with pytest.raises(NotImplementedError):
        resolver.resolve("test")


def test_http_resolver_not_implemented():
    """Test that HTTPResolver raises NotImplementedError."""
    resolver = HTTPResolver("http://example.com")
    with pytest.raises(NotImplementedError):
        resolver.resolve("test")
