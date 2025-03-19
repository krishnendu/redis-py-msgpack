# redis-msgpack

A Python Redis client that uses MessagePack serialization for efficient data storage and retrieval.

[![PyPI version](https://img.shields.io/pypi/v/redis-msgpack.svg)](https://pypi.org/project/redis-msgpack/)
[![Python versions](https://img.shields.io/pypi/pyversions/redis-msgpack.svg)](https://pypi.org/project/redis-msgpack/)
[![License](https://img.shields.io/pypi/l/redis-msgpack.svg)](https://github.com/yourusername/redis-msgpack/blob/main/LICENSE)

## Features

- Drop-in replacement for the standard Redis-py client
- Automatic MessagePack serialization and deserialization
- Significantly reduced memory usage and network transfer
- Full support for all Redis data types and commands
- Django cache backend integration
- Type hints for better IDE support

## Installation

```bash
pip install redis-msgpack
```

With Django integration:

```bash
pip install redis-msgpack[django]
```

## Basic Usage

```python
from redis_msgpack import RedisMsgpackClient

# Connect to Redis
client = RedisMsgpackClient(host='localhost', port=6379, db=0)

# Store Python objects directly - they will be serialized with msgpack
client.set('user:profile', {
    'username': 'johndoe',
    'email': 'john@example.com',
    'preferences': ['dark_mode', 'notifications_on'],
    'login_count': 42,
    'last_login': 1647853426.78
})

# Retrieve and automatically deserialize
user_profile = client.get('user:profile')
print(user_profile['username'])  # 'johndoe'
```

## Django Integration

Add to your Django settings:

```python
CACHES = {
    "default": {
        "BACKEND": "redis_msgpack.django_integration.RedisMsgpackCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "CLIENT_CLASS": "redis_msgpack.client.RedisMsgpackClient",
            "SERIALIZER_CLASS": "redis_msgpack.utils.MsgpackSerializer",
        }
    }
}
```

Then use Django's cache as usual:

```python
from django.core.cache import cache

# Store complex data
cache.set('key', {'complex': 'data', 'numbers': [1, 2, 3]})

# Retrieve data
data = cache.get('key')
```

## Advanced Usage

### Pipeline Support

```python
with client.pipeline() as pipe:
    pipe.set('key1', 'value1')
    pipe.set('key2', [1, 2, 3, 4])
    pipe.hset('hash1', mapping={'field1': 'value1', 'field2': 'value2'})
    result = pipe.execute()
```

### PubSub Support

```python
pubsub = client.pubsub()
pubsub.subscribe('channel1')

# Process messages
for message in pubsub.listen():
    if message['type'] == 'message':
        print(f"Received: {message['data']}")
```

### Custom Serialization Options

```python
from redis_msgpack import RedisMsgpackClient
from redis_msgpack.utils import MsgpackSerializer

# Configure custom serialization options
serializer = MsgpackSerializer(use_bin_type=True, use_single_float=False)
client = RedisMsgpackClient(host='localhost', port=6379, serializer=serializer)
```

## Performance Comparison

When compared to standard JSON serialization, redis-msgpack typically provides:

- 20-30% smaller serialized data size
- 10-20% faster serialization/deserialization
- Full support for binary data without additional encoding

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.