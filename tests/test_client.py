"""
Tests for the RedisMsgpackClient class.
"""
import pytest
import time
import pickle
from redis_msgpack import RedisMsgpackClient
from redis_msgpack.utils import MsgpackSerializer
import msgpack


def test_basic_set_get(redis_msgpack_client):
    """Test basic set and get operations."""
    # Simple string
    redis_msgpack_client.set("test_string", "hello world")
    assert redis_msgpack_client.get("test_string") == "hello world"
    
    # Integer
    redis_msgpack_client.set("test_int", 42)
    assert redis_msgpack_client.get("test_int") == 42
    
    # Float
    redis_msgpack_client.set("test_float", 3.14159)
    assert redis_msgpack_client.get("test_float") == 3.14159
    
    # Boolean
    redis_msgpack_client.set("test_bool", True)
    assert redis_msgpack_client.get("test_bool") is True
    
    # None
    redis_msgpack_client.set("test_none", None)
    assert redis_msgpack_client.get("test_none") is None


def test_complex_data_types(redis_msgpack_client, complex_test_data):
    """Test serialization of complex data types."""
    # Set complex data
    redis_msgpack_client.set("complex_data", complex_test_data)
    
    # Get and verify
    result = redis_msgpack_client.get("complex_data")
    
    # Basic types
    assert result["string"] == complex_test_data["string"]
    assert result["integer"] == complex_test_data["integer"]
    assert result["float"] == complex_test_data["float"]
    assert result["boolean"] == complex_test_data["boolean"]
    assert result["none"] == complex_test_data["none"]
    
    # Collections
    assert result["list"] == complex_test_data["list"]
    assert result["dict"] == complex_test_data["dict"]
    
    # Nested structures
    assert result["nested"]["list"] == complex_test_data["nested"]["list"]
    assert result["nested"]["dict"] == complex_test_data["nested"]["dict"]
    
    # Special types
    assert result["datetime"] == complex_test_data["datetime"]
    assert result["date"] == complex_test_data["date"]
    assert result["time"] == complex_test_data["time"]
    assert result["decimal"] == complex_test_data["decimal"]
    assert result["uuid"] == complex_test_data["uuid"]
    assert result["set"] == complex_test_data["set"]
    assert result["frozenset"] == complex_test_data["frozenset"]
    assert result["bytes"] == complex_test_data["bytes"]


def test_pipeline(redis_msgpack_client):
    """Test pipeline operations."""
    # Create a pipeline
    with redis_msgpack_client.pipeline() as pipe:
        pipe.set("pipe_str", "string value")
        pipe.set("pipe_list", [1, 2, 3, 4])
        pipe.set("pipe_dict", {"key": "value"})
        pipe.get("pipe_str")
        pipe.get("pipe_list")
        pipe.get("pipe_dict")
        results = pipe.execute()
    
    # Check results
    assert results[0]  # set returns True
    assert results[1]  # set returns True
    assert results[2]  # set returns True
    assert results[3] == "string value"
    assert results[4] == [1, 2, 3, 4]
    assert results[5] == {"key": "value"}


def test_expiration(redis_msgpack_client):
    """Test key expiration."""
    # Set with expiry
    redis_msgpack_client.set("expire_key", "value", ex=1)
    
    # Key should exist initially
    assert redis_msgpack_client.exists("expire_key") == 1
    
    # Wait for expiration
    time.sleep(1.5)
    
    # Key should be gone
    assert redis_msgpack_client.exists("expire_key") == 0


def test_list_operations(redis_msgpack_client):
    """Test list operations."""
    # Push items to a list
    redis_msgpack_client.lpush("test_list", "item1")
    redis_msgpack_client.lpush("test_list", "item2")
    redis_msgpack_client.rpush("test_list", "item3")
    
    # Get list length
    assert redis_msgpack_client.llen("test_list") == 3
    
    # Get list range
    assert redis_msgpack_client.lrange("test_list", 0, -1) == ["item2", "item1", "item3"]
    
    # Pop items
    assert redis_msgpack_client.lpop("test_list") == "item2"
    assert redis_msgpack_client.rpop("test_list") == "item3"
    assert redis_msgpack_client.lpop("test_list") == "item1"


def test_hash_operations(redis_msgpack_client):
    """Test hash operations."""
    # Set hash fields
    redis_msgpack_client.hset("test_hash", "field1", "value1")
    redis_msgpack_client.hset("test_hash", "field2", 42)
    redis_msgpack_client.hset("test_hash", "field3", [1, 2, 3])
    
    # Get hash fields
    assert redis_msgpack_client.hget("test_hash", "field1") == "value1"
    assert redis_msgpack_client.hget("test_hash", "field2") == 42
    assert redis_msgpack_client.hget("test_hash", "field3") == [1, 2, 3]
    
    # Get all hash
    expected = {"field1": "value1", "field2": 42, "field3": [1, 2, 3]}
    assert redis_msgpack_client.hgetall("test_hash") == expected


def test_set_operations(redis_msgpack_client):
    """Test set operations."""
    # Add members
    redis_msgpack_client.sadd("test_set", "member1")
    redis_msgpack_client.sadd("test_set", "member2")
    redis_msgpack_client.sadd("test_set", "member3")
    
    # Check membership
    assert redis_msgpack_client.sismember("test_set", "member1")
    assert redis_msgpack_client.sismember("test_set", "member4") == 0
    
    # Get all members
    members = redis_msgpack_client.smembers("test_set")
    assert len(members) == 3
    assert "member1" in members
    assert "member2" in members
    assert "member3" in members


def test_sorted_set_operations(redis_msgpack_client):
    """Test sorted set operations."""
    # Add members with scores
    redis_msgpack_client.zadd("test_zset", {"member1": 1.0, "member2": 2.0, "member3": 3.0})
    
    # Get all members
    members = redis_msgpack_client.zrange("test_zset", 0, -1, withscores=True)
    assert len(members) == 3
    assert members[0][0] == "member1" and members[0][1] == 1.0
    assert members[1][0] == "member2" and members[1][1] == 2.0
    assert members[2][0] == "member3" and members[2][1] == 3.0
    
    # Get members by score
    members = redis_msgpack_client.zrangebyscore("test_zset", 2.0, 3.0)
    assert len(members) == 2
    assert "member2" in members
    assert "member3" in members


def test_pubsub(redis_msgpack_client):
    """Test publish/subscribe functionality."""
    # Create a pubsub
    pubsub = redis_msgpack_client.pubsub()
    
    # Subscribe to a channel
    pubsub.subscribe("test_channel")
    
    # Get subscription confirmation message
    message = pubsub.get_message()
    assert message["type"] == "subscribe"
    assert message["channel"] == "test_channel"
    
    # Publish a message
    redis_msgpack_client.publish("test_channel", "test message")
    
    # Get the message
    message = pubsub.get_message()
    assert message["type"] == "message"
    assert message["channel"] == "test_channel"
    assert message["data"] == "test message"
    
    # Publish complex data
    redis_msgpack_client.publish("test_channel", {"key": "value", "list": [1, 2, 3]})
    
    # Get the complex message
    message = pubsub.get_message()
    assert message["type"] == "message"
    assert message["channel"] == "test_channel"
    assert message["data"] == {"key": "value", "list": [1, 2, 3]}
    
    # Unsubscribe
    pubsub.unsubscribe("test_channel")
    message = pubsub.get_message()
    assert message["type"] == "unsubscribe"
    assert message["channel"] == "test_channel"


def test_serializer_customization():
    """Test customizing the serializer."""
    # Create custom serializer with different options
    serializer = MsgpackSerializer(pack_options={"use_bin_type": True})
    
    # Create client with custom serializer
    client = RedisMsgpackClient(
        host="localhost",
        port=6379,
        db=15,
        serializer=serializer
    )
    
    # Test serialization with custom serializer
    client.set("custom_serializer_test", {"data": [1, 2, 3]})
    assert client.get("custom_serializer_test") == {"data": [1, 2, 3]}
    
    # Clean up
    client.delete("custom_serializer_test")
    client.close()


def test_compare_with_pickle(redis_connection, redis_msgpack_client, complex_test_data):
    """Compare MessagePack serialization with pickle."""
    # Serialize with pickle
    pickled_data = pickle.dumps(complex_test_data)
    redis_connection.set("pickle_data", pickled_data)
    
    # Serialize with msgpack via our client
    redis_msgpack_client.set("msgpack_data", complex_test_data)
    
    # Get raw data from Redis
    pickle_raw = redis_connection.get("pickle_data")
    msgpack_raw = redis_connection.get("msgpack_data")
    
    # Compare sizes (MessagePack should be smaller or equal)
    print(f"Pickle size: {len(pickle_raw)} bytes")
    print(f"MessagePack size: {len(msgpack_raw)} bytes")
    
    # Deserialize pickle data manually
    pickle_deserialized = pickle.loads(pickle_raw)
    
    # Get the msgpack data through our client
    msgpack_deserialized = redis_msgpack_client.get("msgpack_data")
    
    # Verify both serializations produced correct results
    assert pickle_deserialized["string"] == msgpack_deserialized["string"]
    assert pickle_deserialized["integer"] == msgpack_deserialized["integer"]
    assert pickle_deserialized["float"] == msgpack_deserialized["float"]
    assert pickle_deserialized["list"] == msgpack_deserialized["list"]
    assert pickle_deserialized["dict"] == msgpack_deserialized["dict"]


def test_large_data_handling(redis_msgpack_client):
    """Test handling of large data structures."""
    # Create a large list
    large_list = list(range(10000))
    redis_msgpack_client.set("large_list", large_list)
    assert redis_msgpack_client.get("large_list") == large_list
    
    # Create a large dictionary
    large_dict = {f"key_{i}": f"value_{i}" for i in range(1000)}
    redis_msgpack_client.set("large_dict", large_dict)
    assert redis_msgpack_client.get("large_dict") == large_dict


def test_binary_data(redis_msgpack_client):
    """Test handling of binary data."""
    # Binary data
    binary_data = b"\x00\x01\x02\x03\xFF\xFE\xFD\xFC"
    redis_msgpack_client.set("binary_data", binary_data)
    assert redis_msgpack_client.get("binary_data") == binary_data
    
    # Mixed data with binary
    mixed_data = {
        "text": "hello world",
        "binary": b"\x00\x01\x02\x03",
        "list": [b"\xFF\xFE", b"\xFD\xFC"]
    }
    redis_msgpack_client.set("mixed_binary", mixed_data)
    assert redis_msgpack_client.get("mixed_binary") == mixed_data


def test_error_handling(redis_msgpack_client):
    """Test error handling in the client."""
    # Test invalid key type
    with pytest.raises(TypeError):
        redis_msgpack_client.set(123, "value")
    
    # Test connection error (simulate by closing connection)
    redis_msgpack_client.close()
    with pytest.raises(Exception):
        redis_msgpack_client.set("test", "value")