"""
Pytest configuration for redis-msgpack tests.
"""
import os
import pytest
import redis
from redis_msgpack import RedisMsgpackClient
from redis_msgpack.utils import MsgpackSerializer

# Get Redis connection details from environment or use defaults
REDIS_HOST = os.environ.get("REDIS_TEST_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_TEST_PORT", "6379"))
REDIS_DB = int(os.environ.get("REDIS_TEST_DB", "15"))  # Use DB 15 for tests by default


@pytest.fixture(scope="session")
def redis_connection():
    """
    Create a standard Redis client for test setup and teardown.
    """
    client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=False
    )
    
    # Make sure we can connect
    client.ping()
    
    yield client
    
    # Clean up after tests
    client.flushdb()
    client.close()


@pytest.fixture(scope="function")
def redis_msgpack_client(redis_connection):
    """
    Create a RedisMsgpackClient for testing.
    """
    # Flush the test database before each test to ensure clean state
    redis_connection.flushdb()
    
    client = RedisMsgpackClient(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB
    )
    
    yield client
    
    # Clean up
    client.close()


@pytest.fixture(scope="function")
def msgpack_serializer():
    """
    Create a MsgpackSerializer for testing.
    """
    return MsgpackSerializer()


@pytest.fixture(scope="function")
def complex_test_data():
    """
    Provide a complex nested data structure for testing serialization.
    """
    import datetime
    import decimal
    import uuid
    
    return {
        "string": "hello world",
        "integer": 42,
        "float": 3.14159,
        "boolean": True,
        "none": None,
        "list": [1, 2, 3, 4, 5],
        "dict": {"key1": "value1", "key2": "value2"},
        "nested": {
            "list": [1, [2, 3], 4],
            "dict": {"key": [1, 2, {"inner": "value"}]}
        },
        "datetime": datetime.datetime(2025, 3, 19, 12, 0, 0),
        "date": datetime.date(2025, 3, 19),
        "time": datetime.time(12, 0, 0),
        "decimal": decimal.Decimal("3.14159265358979323846"),
        "uuid": uuid.uuid4(),
        "set": {1, 2, 3, 4, 5},
        "frozenset": frozenset([1, 2, 3, 4, 5]),
        "bytes": b"binary data",
    }


try:
    import django
    from django.conf import settings
    
    # Configure Django settings for tests if Django is available
    @pytest.fixture(scope="session", autouse=True)
    def django_setup():
        if not settings.configured:
            settings.configure(
                INSTALLED_APPS=[
                    "django.contrib.auth",
                    "django.contrib.contenttypes",
                ],
                DATABASES={
                    "default": {
                        "ENGINE": "django.db.backends.sqlite3",
                        "NAME": ":memory:",
                    }
                },
                CACHES={
                    "default": {
                        "BACKEND": "redis_msgpack.django_integration.RedisMsgpackCache",
                        "LOCATION": f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
                        "OPTIONS": {
                            "CLIENT_CLASS": "redis_msgpack.client.RedisMsgpackClient",
                        }
                    }
                },
            )
            django.setup()
            
    @pytest.fixture(scope="function")
    def django_cache():
        """
        Get the Django cache instance for testing.
        """
        from django.core.cache import cache
        # Clear cache before each test
        cache.clear()
        return cache
        
except ImportError:
    # Django not available, skip Django-specific tests
    pass