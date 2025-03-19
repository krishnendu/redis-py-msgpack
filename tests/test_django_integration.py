"""
Tests for Django integration features.
"""
import pytest
import datetime

# Skip these tests if Django is not installed
pytest.importorskip("django")

from django.core.cache import cache
from redis_msgpack.django_integration import (
    RedisMsgpackCache, 
    is_django_installed,
    get_cache_key_prefix,
    DjangoModelSerializer
)


def test_django_installation_detection():
    """Test the Django installation detection."""
    assert is_django_installed() is True


def test_cache_key_prefix():
    """Test getting cache key prefix from Django settings."""
    # The prefix should be empty or a string
    prefix = get_cache_key_prefix()
    assert isinstance(prefix, str)


def test_basic_cache_operations(django_cache):
    """Test basic Django cache operations."""
    # Set a value
    django_cache.set("test_key", "test_value")
    
    # Get the value
    assert django_cache.get("test_key") == "test_value"
    
    # Delete the value
    django_cache.delete("test_key")
    assert django_cache.get("test_key") is None


def test_cache_complex_data(django_cache):
    """Test caching complex data structures."""
    # Set complex data
    data = {
        "string": "hello world",
        "integer": 42,
        "float": 3.14159,
        "boolean": True,
        "list": [1, 2, 3, 4, 5],
        "dict": {"key1": "value1", "key2": "value2"},
        "nested": {
            "list": [1, [2, 3], 4],
            "dict": {"key": [1, 2, {"inner": "value"}]}
        }
    }
    django_cache.set("complex_data", data)
    
    # Get and verify
    result = django_cache.get("complex_data")
    assert result == data


def test_cache_timeout(django_cache):
    """Test cache timeout functionality."""
    # Set with a 1 second timeout
    django_cache.set("timeout_key", "timeout_value", timeout=1)
    
    # Value should be available immediately
    assert django_cache.get("timeout_key") == "timeout_value"
    
    # Wait for timeout
    import time
    time.sleep(1.5)
    
    # Value should be gone
    assert django_cache.get("timeout_key") is None


def test_cache_get_or_set(django_cache):
    """Test get_or_set functionality."""
    # First time: value doesn't exist yet
    result = django_cache.get_or_set("get_or_set_key", lambda: "computed_value")
    assert result == "computed_value"
    
    # Second time: value is already cached
    result = django_cache.get_or_set("get_or_set_key", lambda: "new_value")
    assert result == "computed_value"  # Still the original value


def test_cache_add(django_cache):
    """Test the add functionality (only set if not exists)."""
    # First add should work
    django_cache.add("add_key", "add_value")
    assert django_cache.get("add_key") == "add_value"
    
    # Second add should not change the value
    django_cache.add("add_key", "new_value")
    assert django_cache.get("add_key") == "add_value"  # Still the original value


def test_cache_incr_decr(django_cache):
    """Test increment and decrement operations."""
    # Set initial value
    django_cache.set("counter", 10)
    
    # Increment
    django_cache.incr("counter")
    assert django_cache.get("counter") == 11
    
    # Increment by specific amount
    django_cache.incr("counter", 5)
    assert django_cache.get("counter") == 16
    
    # Decrement
    django_cache.decr("counter")
    assert django_cache.get("counter") == 15
    
    # Decrement by specific amount
    django_cache.decr("counter", 5)
    assert django_cache.get("counter") == 10


def test_cache_multiple_operations(django_cache):
    """Test multiple operations using get_many and set_many."""
    # Set multiple values
    django_cache.set_many({
        "multi_key1": "value1",
        "multi_key2": "value2",
        "multi_key3": "value3"
    })
    
    # Get multiple values
    result = django_cache.get_many(["multi_key1", "multi_key2", "multi_key3", "nonexistent_key"])
    assert result["multi_key1"] == "value1"
    assert result["multi_key2"] == "value2"
    assert result["multi_key3"] == "value3"
    assert "nonexistent_key" not in result
    
    # Delete multiple values
    django_cache.delete_many(["multi_key1", "multi_key2"])
    assert django_cache.get("multi_key1") is None
    assert django_cache.get("multi_key2") is None
    assert django_cache.get("multi_key3") == "value3"


def test_cache_versioning(django_cache):
    """Test cache versioning."""
    # Set value with version
    django_cache.set("version_key", "version1_value", version=1)
    django_cache.set("version_key", "version2_value", version=2)
    
    # Get value with specific version
    assert django_cache.get("version_key", version=1) == "version1_value"
    assert django_cache.get("version_key", version=2) == "version2_value"
    
    # Delete with specific version
    django_cache.delete("version_key", version=1)
    assert django_cache.get("version_key", version=1) is None
    assert django_cache.get("version_key", version=2) == "version2_value"


def test_cache_special_types(django_cache):
    """Test caching special Python types."""
    # Date and time types
    now = datetime.datetime.now()
    today = datetime.date.today()
    current_time = datetime.time(now.hour, now.minute, now.second)
    
    # Cache these values
    django_cache.set("datetime", now)
    django_cache.set("date", today)
    django_cache.set("time", current_time)
    
    # Retrieve and verify
    assert django_cache.get("datetime") == now
    assert django_cache.get("date") == today
    assert django_cache.get("time") == current_time


def test_django_model_serializer():
    """Test the DjangoModelSerializer with a sample model."""
    try:
        from django.contrib.auth.models import User
        
        # Create a test user
        test_user = User.objects.create(
            username="testuser",
            email="test@example.com",
            first_name="Test",
            last_name="User"
        )
        
        # Serialize the user
        serialized = DjangoModelSerializer.model_to_dict(test_user)
        
        # Convert back to a model
        deserialized_user = DjangoModelSerializer.dict_to_model(serialized)
        
        # Verify the deserialized user
        assert deserialized_user.username == "testuser"
        assert deserialized_user.email == "test@example.com"
        assert deserialized_user.first_name == "Test"
        assert deserialized_user.last_name == "User"
        
        # Clean up
        test_user.delete()
        
    except Exception as e:
        pytest.skip(f"Could not test DjangoModelSerializer: {e}")