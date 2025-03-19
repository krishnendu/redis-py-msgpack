import msgpack
import redis
from typing import Any, Dict, List, Optional, Set, Tuple, Union


class RedisMsgpackClient:
    """
    Redis client with MessagePack serialization for efficient data storage and retrieval.
    
    This client wraps the standard Redis-py client but serializes all values using MessagePack
    before storing them and deserializes them when retrieving, providing more efficient storage
    and better support for complex Python data types.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        username: Optional[str] = None,
        password: Optional[str] = None,
        socket_timeout: Optional[float] = None,
        socket_connect_timeout: Optional[float] = None,
        socket_keepalive: Optional[bool] = None,
        socket_keepalive_options: Optional[Dict[int, Union[int, bytes]]] = None,
        connection_pool: Optional[redis.ConnectionPool] = None,
        unix_socket_path: Optional[str] = None,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        decode_responses: bool = False,
        retry_on_timeout: bool = False,
        ssl: bool = False,
        ssl_keyfile: Optional[str] = None,
        ssl_certfile: Optional[str] = None,
        ssl_cert_reqs: Optional[str] = None,
        ssl_ca_certs: Optional[str] = None,
        ssl_check_hostname: bool = False,
        max_connections: Optional[int] = None,
        single_connection_client: bool = False,
        health_check_interval: int = 0,
        client_name: Optional[str] = None,
        redis_connect_func: Optional[callable] = None,
    ):
        """
        Initialize the Redis client with MessagePack serialization.
        
        Args:
            host: Redis server host
            port: Redis server port
            db: Redis database number
            username: Redis username for authentication
            password: Redis password for authentication
            socket_timeout: Socket timeout in seconds
            socket_connect_timeout: Socket connect timeout in seconds
            socket_keepalive: Whether to use socket keepalive
            socket_keepalive_options: Socket keepalive options
            connection_pool: An existing Redis connection pool
            unix_socket_path: Path to a Unix domain socket connection
            encoding: Encoding to use for Redis commands
            encoding_errors: How to handle encoding errors
            decode_responses: Whether to decode string responses
            retry_on_timeout: Whether to retry on timeout
            ssl: Whether to use SSL
            ssl_keyfile: Path to SSL key file
            ssl_certfile: Path to SSL certificate file
            ssl_cert_reqs: SSL certificate requirements
            ssl_ca_certs: Path to SSL CA certificates file
            ssl_check_hostname: Whether to check hostname
            max_connections: Maximum number of connections in the pool
            single_connection_client: Whether to use a single connection client
            health_check_interval: Health check interval in seconds
            client_name: Client name to be sent to Redis
            redis_connect_func: Custom Redis connection function
        """
        self.client = redis.Redis(
            host=host,
            port=port,
            db=db,
            username=username,
            password=password,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            socket_keepalive=socket_keepalive,
            socket_keepalive_options=socket_keepalive_options,
            connection_pool=connection_pool,
            unix_socket_path=unix_socket_path,
            encoding=encoding,
            encoding_errors=encoding_errors,
            decode_responses=decode_responses,
            retry_on_timeout=retry_on_timeout,
            ssl=ssl,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
            ssl_cert_reqs=ssl_cert_reqs,
            ssl_ca_certs=ssl_ca_certs,
            ssl_check_hostname=ssl_check_hostname,
            max_connections=max_connections,
            single_connection_client=single_connection_client,
            health_check_interval=health_check_interval,
            client_name=client_name,
            redis_connect_func=redis_connect_func,
        )

    ### -------------------- STRING OPERATIONS -------------------- ###

    def set(self, key: str, value: Any, ex: Optional[int] = None, px: Optional[int] = None, 
            nx: bool = False, xx: bool = False, keepttl: bool = False) -> bool:
        """
        Set key to hold the packed value.
        
        Args:
            key: Key to set
            value: Value to store (will be packed with msgpack)
            ex: Expire time in seconds
            px: Expire time in milliseconds
            nx: Only set if key does not exist
            xx: Only set if key exists
            keepttl: Retain the time to live associated with the key
            
        Returns:
            True if SET was executed correctly, False otherwise
        """
        packed_value = msgpack.packb(value)
        return self.client.set(key, packed_value, ex=ex, px=px, nx=nx, xx=xx, keepttl=keepttl)

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get the value at key.
        
        Args:
            key: Key to retrieve
            default: Default value to return if key doesn't exist
            
        Returns:
            Unpacked value if key exists, default otherwise
        """
        packed_value = self.client.get(key)
        return msgpack.unpackb(packed_value) if packed_value else default

    def mset(self, mapping: Dict[str, Any]) -> bool:
        """
        Set multiple keys to multiple values.
        
        Args:
            mapping: Dict of key-value pairs to set
            
        Returns:
            True if successful
        """
        packed_mapping = {k: msgpack.packb(v) for k, v in mapping.items()}
        return self.client.mset(packed_mapping)

    def mget(self, keys: List[str], default: Any = None) -> List[Any]:
        """
        Get the values of all specified keys.
        
        Args:
            keys: List of keys to retrieve
            default: Default value for keys that don't exist
            
        Returns:
            List of unpacked values, with default value for missing keys
        """
        packed_values = self.client.mget(keys)
        return [msgpack.unpackb(v) if v else default for v in packed_values]

    def getset(self, key: str, value: Any) -> Any:
        """
        Set the string value of a key and return its old value.
        
        Args:
            key: Key to set
            value: New value to set
            
        Returns:
            Old value or None if key didn't exist
        """
        packed_value = msgpack.packb(value)
        old_packed_value = self.client.getset(key, packed_value)
        return msgpack.unpackb(old_packed_value) if old_packed_value else None

    def incr(self, key: str, amount: int = 1) -> int:
        """
        Increment the integer value of a key by the given amount.
        
        This operation requires special handling with msgpack, as we need
        to get-modify-set rather than using Redis's native INCR.
        
        Args:
            key: Key to increment
            amount: Amount to increment by
            
        Returns:
            New value after incrementing
        """
        value = self.get(key, 0)
        if not isinstance(value, int):
            raise TypeError(f"Cannot increment non-integer value: {type(value)}")
        
        new_value = value + amount
        self.set(key, new_value)
        return new_value

    def decr(self, key: str, amount: int = 1) -> int:
        """
        Decrement the integer value of a key by the given amount.
        
        Args:
            key: Key to decrement
            amount: Amount to decrement by
            
        Returns:
            New value after decrementing
        """
        return self.incr(key, -amount)

    ### -------------------- HASH OPERATIONS -------------------- ###

    def hset(self, key: str, field: str, value: Any) -> int:
        """
        Set a single field in a hash with msgpack serialization.
        
        Args:
            key: Hash key
            field: Field name
            value: Value to set (will be packed)
            
        Returns:
            1 if field is a new field and value was set, 0 if field existed and value was updated
        """
        packed_value = msgpack.packb(value)
        return self.client.hset(key, field, packed_value)

    def hmset(self, key: str, mapping: Dict[str, Any]) -> bool:
        """
        Set multiple fields in a hash with msgpack serialization.
        
        Args:
            key: Hash key
            mapping: Dict of field-value pairs to set
            
        Returns:
            True if successful
        """
        packed_mapping = {k: msgpack.packb(v) for k, v in mapping.items()}
        return self.client.hset(key, mapping=packed_mapping)

    def hget(self, key: str, field: str, default: Any = None) -> Any:
        """
        Get a single field from a hash and deserialize it, with a default fallback.
        
        Args:
            key: Hash key
            field: Field name
            default: Default value if field doesn't exist
            
        Returns:
            Unpacked value if field exists, default otherwise
        """
        packed_value = self.client.hget(key, field)
        return msgpack.unpackb(packed_value) if packed_value else default

    def hmget(self, key: str, fields: List[str]) -> List[Any]:
        """
        Get multiple fields from a hash and deserialize them.
        
        Args:
            key: Hash key
            fields: List of field names
            
        Returns:
            List of unpacked values, with None for missing fields
        """
        packed_values = self.client.hmget(key, *fields)
        return [msgpack.unpackb(v) if v else None for v in packed_values]

    def hgetall(self, key: str) -> Dict[str, Any]:
        """
        Get all fields from a hash and deserialize them.
        
        Args:
            key: Hash key
            
        Returns:
            Dict of field-value pairs, empty dict if key doesn't exist
        """
        packed_data = self.client.hgetall(key)
        return {k.decode(): msgpack.unpackb(v) for k, v in packed_data.items()} if packed_data else {}

    def hdel(self, key: str, *fields: str) -> int:
        """
        Delete one or more hash fields.
        
        Args:
            key: Hash key
            fields: Field names to delete
            
        Returns:
            Number of fields that were removed
        """
        return self.client.hdel(key, *fields)

    def hlen(self, key: str) -> int:
        """
        Get the number of fields in a hash.
        
        Args:
            key: Hash key
            
        Returns:
            Number of fields in the hash
        """
        return self.client.hlen(key)

    def hexists(self, key: str, field: str) -> bool:
        """
        Check if a field exists in a hash.
        
        Args:
            key: Hash key
            field: Field name
            
        Returns:
            True if field exists, False otherwise
        """
        return self.client.hexists(key, field)

    def hkeys(self, key: str) -> List[str]:
        """
        Get all field names in a hash.
        
        Args:
            key: Hash key
            
        Returns:
            List of field names
        """
        keys = self.client.hkeys(key)
        return [k.decode() for k in keys]

    def hvals(self, key: str) -> List[Any]:
        """
        Get all values in a hash.
        
        Args:
            key: Hash key
            
        Returns:
            List of unpacked values
        """
        packed_values = self.client.hvals(key)
        return [msgpack.unpackb(v) for v in packed_values]

    def hincrby(self, key: str, field: str, amount: int = 1) -> int:
        """
        Increment the integer value of a hash field by the given amount.
        
        Args:
            key: Hash key
            field: Field name
            amount: Amount to increment by
            
        Returns:
            New value after incrementing
        """
        value = self.hget(key, field, 0)
        if not isinstance(value, int):
            raise TypeError(f"Cannot increment non-integer value: {type(value)}")
        
        new_value = value + amount
        self.hset(key, field, new_value)
        return new_value

    def hincrbyfloat(self, key: str, field: str, amount: float = 1.0) -> float:
        """
        Increment the float value of a hash field by the given amount.
        
        Args:
            key: Hash key
            field: Field name
            amount: Amount to increment by
            
        Returns:
            New value after incrementing
        """
        value = self.hget(key, field, 0.0)
        if not isinstance(value, (int, float)):
            raise TypeError(f"Cannot increment non-numeric value: {type(value)}")
        
        new_value = value + amount
        self.hset(key, field, new_value)
        return new_value

    ### -------------------- SET OPERATIONS -------------------- ###

    def sadd(self, key: str, *values: Any) -> int:
        """
        Add one or more elements to a Redis set.
        
        Args:
            key: Set key
            values: Values to add
            
        Returns:
            Number of elements added to the set
        """
        packed_values = [msgpack.packb(v) for v in values]
        return self.client.sadd(key, *packed_values)

    def smembers(self, key: str) -> Set[Any]:
        """
        Get all members of a Redis set and deserialize them.
        
        Args:
            key: Set key
            
        Returns:
            Set of unpacked values
        """
        packed_values = self.client.smembers(key)
        return {msgpack.unpackb(v) for v in packed_values} if packed_values else set()

    def sismember(self, key: str, value: Any) -> bool:
        """
        Check if a value exists in a Redis set.
        
        Args:
            key: Set key
            value: Value to check
            
        Returns:
            True if value is a member of the set, False otherwise
        """
        packed_value = msgpack.packb(value)
        return bool(self.client.sismember(key, packed_value))

    def srem(self, key: str, *values: Any) -> int:
        """
        Remove one or more elements from a Redis set.
        
        Args:
            key: Set key
            values: Values to remove
            
        Returns:
            Number of elements removed from the set
        """
        packed_values = [msgpack.packb(v) for v in values]
        return self.client.srem(key, *packed_values)

    def spop(self, key: str, count: int = 1) -> Union[Any, List[Any], None]:
        """
        Remove and return random members from a set.
        
        Args:
            key: Set key
            count: Number of members to pop
            
        Returns:
            If count=1, returns a single unpacked value or None
            If count>1, returns a list of unpacked values or empty list
        """
        if count == 1:
            packed_value = self.client.spop(key)
            return msgpack.unpackb(packed_value) if packed_value else None
        
        packed_values = self.client.spop(key, count)
        return [msgpack.unpackb(v) for v in packed_values] if packed_values else []

    def scard(self, key: str) -> int:
        """
        Get the number of members in a set.
        
        Args:
            key: Set key
            
        Returns:
            Number of members in the set
        """
        return self.client.scard(key)

    def sdiff(self, *keys: str) -> Set[Any]:
        """
        Return the difference of the first set and all successive sets.
        
        Args:
            keys: Set keys
            
        Returns:
            Set difference
        """
        packed_values = self.client.sdiff(*keys)
        return {msgpack.unpackb(v) for v in packed_values} if packed_values else set()

    def sinter(self, *keys: str) -> Set[Any]:
        """
        Return the intersection of all specified sets.
        
        Args:
            keys: Set keys
            
        Returns:
            Set intersection
        """
        packed_values = self.client.sinter(*keys)
        return {msgpack.unpackb(v) for v in packed_values} if packed_values else set()

    def sunion(self, *keys: str) -> Set[Any]:
        """
        Return the union of all specified sets.
        
        Args:
            keys: Set keys
            
        Returns:
            Set union
        """
        packed_values = self.client.sunion(*keys)
        return {msgpack.unpackb(v) for v in packed_values} if packed_values else set()

    def srandmember(self, key: str, count: Optional[int] = None) -> Union[Any, List[Any], None]:
        """
        Get one or more random members from a set.
        
        Args:
            key: Set key
            count: Number of members to return
            
        Returns:
            If count is None, returns a single unpacked value or None
            If count is not None, returns a list of unpacked values or empty list
        """
        if count is None:
            packed_value = self.client.srandmember(key)
            return msgpack.unpackb(packed_value) if packed_value else None
        
        packed_values = self.client.srandmember(key, count)
        return [msgpack.unpackb(v) for v in packed_values] if packed_values else []

    ### -------------------- LIST/QUEUE OPERATIONS -------------------- ###

    def rpush(self, key: str, *values: Any) -> int:
        """
        Add one or more values to the end of a list.
        
        Args:
            key: List key
            values: Values to push
            
        Returns:
            Length of the list after the push operation
        """
        packed_values = [msgpack.packb(v) for v in values]
        return self.client.rpush(key, *packed_values)

    def lpush(self, key: str, *values: Any) -> int:
        """
        Add one or more values to the beginning of a list.
        
        Args:
            key: List key
            values: Values to push
            
        Returns:
            Length of the list after the push operation
        """
        packed_values = [msgpack.packb(v) for v in values]
        return self.client.lpush(key, *packed_values)

    def lpop(self, key: str) -> Any:
        """
        Remove and return the first item from a list.
        
        Args:
            key: List key
            
        Returns:
            Unpacked value or None if list is empty
        """
        packed_value = self.client.lpop(key)
        return msgpack.unpackb(packed_value) if packed_value else None

    def rpop(self, key: str) -> Any:
        """
        Remove and return the last item from a list.
        
        Args:
            key: List key
            
        Returns:
            Unpacked value or None if list is empty
        """
        packed_value = self.client.rpop(key)
        return msgpack.unpackb(packed_value) if packed_value else None

    def blpop(self, keys: Union[List[str], Tuple[str, ...]], timeout: int = 0) -> Optional[Tuple[str, Any]]:
        """
        Blocking pop from the beginning of a list.
        
        Args:
            keys: List of keys or single key
            timeout: Timeout in seconds, 0 means block indefinitely
            
        Returns:
            Tuple of (key, value) or None if timeout is reached
        """
        result = self.client.blpop(keys, timeout)
        if result:
            key, packed_value = result
            return key.decode(), msgpack.unpackb(packed_value)
        return None

    def brpop(self, keys: Union[List[str], Tuple[str, ...]], timeout: int = 0) -> Optional[Tuple[str, Any]]:
        """
        Blocking pop from the end of a list.
        
        Args:
            keys: List of keys or single key
            timeout: Timeout in seconds, 0 means block indefinitely
            
        Returns:
            Tuple of (key, value) or None if timeout is reached
        """
        result = self.client.brpop(keys, timeout)
        if result:
            key, packed_value = result
            return key.decode(), msgpack.unpackb(packed_value)
        return None

    def brpoplpush(self, source: str, destination: str, timeout: int = 0) -> Any:
        """
        Pop an item from the end of source and push to the beginning of destination.
        
        This is atomic and blocks if the source is empty.
        
        Args:
            source: Source list key
            destination: Destination list key
            timeout: Timeout in seconds, 0 means block indefinitely
            
        Returns:
            Unpacked value or None if timeout is reached
        """
        packed_value = self.client.brpoplpush(source, destination, timeout)
        return msgpack.unpackb(packed_value) if packed_value else None

    def lrange(self, key: str, start: int, end: int) -> List[Any]:
        """
        Get a range of elements from a list.
        
        Args:
            key: List key
            start: Start index
            end: End index
            
        Returns:
            List of unpacked values
        """
        packed_values = self.client.lrange(key, start, end)
        return [msgpack.unpackb(v) for v in packed_values] if packed_values else []

    def llen(self, key: str) -> int:
        """
        Get the length of a list.
        
        Args:
            key: List key
            
        Returns:
            Length of the list
        """
        return self.client.llen(key)

    def lindex(self, key: str, index: int) -> Any:
        """
        Get an element from a list by its index.
        
        Args:
            key: List key
            index: Index of the element
            
        Returns:
            Unpacked value or None if index is out of range
        """
        packed_value = self.client.lindex(key, index)
        return msgpack.unpackb(packed_value) if packed_value else None

    def lset(self, key: str, index: int, value: Any) -> bool:
        """
        Set the value of an element in a list by its index.
        
        Args:
            key: List key
            index: Index of the element
            value: New value
            
        Returns:
            True if successful
        """
        packed_value = msgpack.packb(value)
        return self.client.lset(key, index, packed_value)

    def lrem(self, key: str, count: int, value: Any) -> int:
        """
        Remove elements from a list.
        
        Count > 0: Remove elements equal to value moving from head to tail.
        Count < 0: Remove elements equal to value moving from tail to head.
        Count = 0: Remove all elements equal to value.
        
        Args:
            key: List key
            count: Count and direction
            value: Value to remove
            
        Returns:
            Number of removed elements
        """
        packed_value = msgpack.packb(value)
        return self.client.lrem(key, count, packed_value)
    
    def lpos(self, key: str, value: Any, rank: Optional[int] = None, 
             count: Optional[int] = None, maxlen: Optional[int] = None) -> Union[int, List[int], None]:
        """
        Find positions of elements in a list matching the specified value.
        
        Args:
            key: List key
            value: Value to search for
            rank: Optional zero-based rank to start searching from
            count: Optional number of matches to return
            maxlen: Optional limit of elements to scan
            
        Returns:
            If count is None: Position of the first match or None
            If count is not None: List of positions or empty list
        """
        packed_value = msgpack.packb(value)
        
        kwargs = {}
        if rank is not None:
            kwargs['rank'] = rank
        if count is not None:
            kwargs['count'] = count
        if maxlen is not None:
            kwargs['maxlen'] = maxlen

        result = self.client.lpos(key, packed_value, **kwargs)
        
        # Result handling depends on whether count was specified
        if count is None:
            # Single position or None
            return result
        else:
            # List of positions or empty list
            return result if result else []

    def ltrim(self, key: str, start: int, end: int) -> bool:
        """
        Trim a list to the specified range.
        
        Args:
            key: List key
            start: Start index
            end: End index
            
        Returns:
            True if successful
        """
        return self.client.ltrim(key, start, end)

    ### -------------------- SORTED SET OPERATIONS -------------------- ###

    def zadd(self, key: str, mapping: Dict[Any, float], nx: bool = False, 
             xx: bool = False, ch: bool = False, incr: bool = False) -> int:
        """
        Add one or more members to a sorted set, or update existing members' scores.
        
        Args:
            key: Sorted set key
            mapping: Dict of {value: score} pairs
            nx: Only add new elements
            xx: Only update existing elements
            ch: Return the number of changed elements
            incr: Increment the score by the provided value
            
        Returns:
            Number of elements added or updated
        """
        packed_mapping = {msgpack.packb(k): v for k, v in mapping.items()}
        return self.client.zadd(key, packed_mapping, nx=nx, xx=xx, ch=ch, incr=incr)

    def zrange(self, key: str, start: int, end: int, desc: bool = False, 
               withscores: bool = False) -> Union[List[Any], List[Tuple[Any, float]]]:
        """
        Return a range of members from a sorted set.
        
        Args:
            key: Sorted set key
            start: Start index
            end: End index
            desc: Descending order
            withscores: Include scores in the result
            
        Returns:
            List of unpacked values or list of (value, score) tuples
        """
        result = self.client.zrange(key, start, end, desc=desc, withscores=withscores)
        
        if withscores:
            return [(msgpack.unpackb(v), score) for v, score in result]
        else:
            return [msgpack.unpackb(v) for v in result]

    def zrangebyscore(self, key: str, min: Union[float, str], max: Union[float, str], 
                      start: Optional[int] = None, num: Optional[int] = None, 
                      withscores: bool = False) -> Union[List[Any], List[Tuple[Any, float]]]:
        """
        Return a range of members from a sorted set by score.
        
        Args:
            key: Sorted set key
            min: Minimum score
            max: Maximum score
            start: Start offset
            num: Number of elements to return
            withscores: Include scores in the result
            
        Returns:
            List of unpacked values or list of (value, score) tuples
        """
        result = self.client.zrangebyscore(key, min, max, start=start, num=num, withscores=withscores)
        
        if withscores:
            return [(msgpack.unpackb(v), score) for v, score in result]
        else:
            return [msgpack.unpackb(v) for v in result]

    def zrem(self, key: str, *values: Any) -> int:
        """
        Remove one or more members from a sorted set.
        
        Args:
            key: Sorted set key
            values: Values to remove
            
        Returns:
            Number of members removed
        """
        packed_values = [msgpack.packb(v) for v in values]
        return self.client.zrem(key, *packed_values)

    def zcard(self, key: str) -> int:
        """
        Get the number of members in a sorted set.
        
        Args:
            key: Sorted set key
            
        Returns:
            Number of members
        """
        return self.client.zcard(key)

    def zscore(self, key: str, value: Any) -> Optional[float]:
        """
        Get the score of a member in a sorted set.
        
        Args:
            key: Sorted set key
            value: Member value
            
        Returns:
            Score of the member or None if member doesn't exist
        """
        packed_value = msgpack.packb(value)
        return self.client.zscore(key, packed_value)

    def zincrby(self, key: str, amount: float, value: Any) -> float:
        """
        Increment the score of a member in a sorted set.
        
        Args:
            key: Sorted set key
            amount: Increment by this amount
            value: Member value
            
        Returns:
            New score after incrementing
        """
        packed_value = msgpack.packb(value)
        return self.client.zincrby(key, amount, packed_value)

    def zrank(self, key: str, value: Any) -> Optional[int]:
        """
        Determine the index of a member in a sorted set (in ascending order).
        
        Args:
            key: Sorted set key
            value: Member value
            
        Returns:
            Rank of the member or None if member doesn't exist
        """
        packed_value = msgpack.packb(value)
        return self.client.zrank(key, packed_value)

    def zrevrank(self, key: str, value: Any) -> Optional[int]:
        """
        Determine the index of a member in a sorted set (in descending order).
        
        Args:
            key: Sorted set key
            value: Member value
            
        Returns:
            Rank of the member or None if member doesn't exist
        """
        packed_value = msgpack.packb(value)
        return self.client.zrevrank(key, packed_value)

    def zcount(self, key: str, min: Union[float, str], max: Union[float, str]) -> int:
        """
        Count the number of members in a sorted set with scores within the given range.
        
        Args:
            key: Sorted set key
            min: Minimum score
            max: Maximum score
            
        Returns:
            Number of members in the specified score range
        """
        return self.client.zcount(key, min, max)
    
    ### -------------------- GENERIC KEY OPERATIONS -------------------- ###

    def keys(self, pattern: str) -> List[str]:
        """
        Find all keys matching the given pattern.
        
        Args:
            pattern: Pattern to match
            
        Returns:
            List of matching keys
        """
        keys = self.client.keys(pattern)
        return [k.decode() for k in keys]

    def exists(self, *keys: str) -> int:
        """
        Check if one or more keys exist.
        
        Args:
            keys: Keys to check
            
        Returns:
            Number of keys that exist
        """
        return self.client.exists(*keys)

    def delete(self, *keys: str) -> int:
        """
        Delete one or more keys.
        
        Args:
            keys: Keys to delete
            
        Returns:
            Number of keys that were deleted
        """
        return self.client.delete(*keys)

    def rename(self, src: str, dst: str) -> bool:
        """
        Rename a key.
        
        Args:
            src: Source key
            dst: Destination key
            
        Returns:
            True if successful
        """
        return self.client.rename(src, dst)

    def renamenx(self, src: str, dst: str) -> bool:
        """
        Rename a key, only if the new key does not exist.
        
        Args:
            src: Source key
            dst: Destination key
            
        Returns:
            True if successful
        """
        return self.client.renamenx(src, dst)

    def type(self, key: str) -> str:
        """
        Determine the type stored at key.
        
        Args:
            key: Key to check
            
        Returns:
            Type of value stored at key
        """
        return self.client.type(key)

    def ttl(self, key: str) -> int:
        """
        Get the time to live for a key in seconds.
        
        Args:
            key: Key to check
            
        Returns:
            TTL in seconds, -1 if no expire is set, -2 if key does not exist
        """
        return self.client.ttl(key)

    def pttl(self, key: str) -> int:
        """
        Get the time to live for a key in milliseconds.
        
        Args:
            key: Key to check
            
        Returns:
            TTL in milliseconds, -1 if no expire is set, -2 if key does not exist
        """
        return self.client.pttl(key)

    def expire(self, key: str, time: int) -> bool:
        """
        Set a key's time to live in seconds.
        
        Args:
            key: Key to set expire on
            time: Time to live in seconds
            
        Returns:
            True if the timeout was set, False if key does not exist or timeout could not be set
        """
        return self.client.expire(key, time)

    def pexpire(self, key: str, time: int) -> bool:
        """
        Set a key's time to live in milliseconds.
        
        Args:
            key: Key to set expire on
            time: Time to live in milliseconds
            
        Returns:
            True if the timeout was set, False if key does not exist or timeout could not be set
        """
        return self.client.pexpire(key, time)

    def expireat(self, key: str, when: int) -> bool:
        """
        Set the expiration for a key as a UNIX timestamp.
        
        Args:
            key: Key to set expire on
            when: UNIX timestamp in seconds
            
        Returns:
            True if the timeout was set, False if key does not exist or timeout could not be set
        """
        return self.client.expireat(key, when)

    def pexpireat(self, key: str, when: int) -> bool:
        """
        Set the expiration for a key as a UNIX timestamp in milliseconds.
        
        Args:
            key: Key to set expire on
            when: UNIX timestamp in milliseconds
            
        Returns:
            True if the timeout was set, False if key does not exist or timeout could not be set
        """
        return self.client.pexpireat(key, when)

    def persist(self, key: str) -> bool:
        """
        Remove the expiration from a key.
        
        Args:
            key: Key to remove expiration from
            
        Returns:
            True if the timeout was removed, False if key does not exist or does not have an timeout
        """
        return self.client.persist(key)

    def scan(self, cursor: int = 0, match: str = None, count: int = None, _type: str = None) -> Tuple[int, List[str]]:
        """
        Incrementally iterate the keys space.
        
        Args:
            cursor: Cursor to start at
            match: Pattern to match
            count: Number of elements to return per call
            _type: Filter by type
            
        Returns:
            Tuple of (next cursor, list of keys)
        """
        cursor, keys = self.client.scan(cursor, match=match, count=count, _type=_type)
        return cursor, [k.decode() for k in keys]

    ### -------------------- PIPELINE OPERATIONS -------------------- ###

    def pipeline(self, transaction: bool = True) -> 'RedisMsgpackPipeline':
        """
        Return a new pipeline object that can queue multiple commands for later execution.
        
        Args:
            transaction: Whether to wrap the pipeline in a transaction
            
        Returns:
            New pipeline object
        """
        return RedisMsgpackPipeline(self.client.pipeline(transaction=transaction), self)

    ### -------------------- PUBLISH/SUBSCRIBE OPERATIONS -------------------- ###

    def publish(self, channel: str, message: Any) -> int:
        """
        Publish a message to a channel.
        
        Args:
            channel: Channel to publish to
            message: Message to publish (will be packed with msgpack)
            
        Returns:
            Number of clients that received the message
        """
        packed_message = msgpack.packb(message)
        return self.client.publish(channel, packed_message)

    def subscribe(self, *channels: str) -> None:
        """
        Subscribe to channels.
        
        Note: This is a blocking operation. For handling messages, use the pubsub() method instead.
        
        Args:
            channels: Channels to subscribe to
        """
        self.client.subscribe(*channels)

    def unsubscribe(self, *channels: str) -> None:
        """
        Unsubscribe from channels.
        
        Args:
            channels: Channels to unsubscribe from
        """
        self.client.unsubscribe(*channels)

    def pubsub(self) -> 'RedisMsgpackPubSub':
        """
        Return a pubsub object that can subscribe to channels and listen for messages.
        
        Returns:
            PubSub object with MessagePack support
        """
        return RedisMsgpackPubSub(self.client.pubsub())

    ### -------------------- TRANSACTION OPERATIONS -------------------- ###

    def watch(self, *keys: str) -> bool:
        """
        Watch the given keys to determine execution of the MULTI/EXEC block.
        
        Args:
            keys: Keys to watch
            
        Returns:
            True if successful
        """
        return self.client.watch(*keys)

    def unwatch(self) -> bool:
        """
        Forget about all watched keys.
        
        Returns:
            True if successful
        """
        return self.client.unwatch()

    def multi(self) -> 'redis.client.Pipeline':
        """
        Start a transaction.
        
        Returns:
            Transaction object
        """
        return self.client.multi()

    def execute(self) -> List[Any]:
        """
        Execute a transaction.
        
        Returns:
            List of results
        """
        return self.client.execute()

    ### -------------------- GEO OPERATIONS -------------------- ###

    def geoadd(self, key: str, longitude_latitude_member_mapping: Dict[Any, Tuple[float, float]]) -> int:
        """
        Add one or more geospatial items to the specified key.
        
        Args:
            key: Key to add to
            longitude_latitude_member_mapping: Dict of {member: (longitude, latitude)}
            
        Returns:
            Number of elements added to the set
        """
        packed_mapping = {}
        for member, (longitude, latitude) in longitude_latitude_member_mapping.items():
            packed_mapping[msgpack.packb(member)] = (longitude, latitude)
        
        # Flatten the mapping for redis-py API
        flattened = []
        for member, (longitude, latitude) in packed_mapping.items():
            flattened.extend([longitude, latitude, member])
            
        return self.client.geoadd(key, *flattened)

    def geodist(self, key: str, member1: Any, member2: Any, unit: str = 'm') -> Optional[float]:
        """
        Return the distance between two members of a geospatial index.
        
        Args:
            key: Key to query
            member1: First member
            member2: Second member
            unit: Unit of distance ('m', 'km', 'mi', 'ft')
            
        Returns:
            Distance in specified unit or None if one of the members does not exist
        """
        packed_member1 = msgpack.packb(member1)
        packed_member2 = msgpack.packb(member2)
        return self.client.geodist(key, packed_member1, packed_member2, unit)

    def geopos(self, key: str, *members: Any) -> List[Optional[Tuple[float, float]]]:
        """
        Return the position of one or more members of a geospatial index.
        
        Args:
            key: Key to query
            members: Members to get positions for
            
        Returns:
            List of positions or None for members that don't exist
        """
        packed_members = [msgpack.packb(m) for m in members]
        positions = self.client.geopos(key, *packed_members)
        
        # Redis returns None for non-existing members
        return positions

    def georadius(self, key: str, longitude: float, latitude: float, radius: float, unit: str = 'm',
                  withdist: bool = False, withcoord: bool = False, withhash: bool = False,
                  count: Optional[int] = None, sort: Optional[str] = None, store: Optional[str] = None,
                  storedist: Optional[str] = None) -> List[Any]:
        """
        Return members of a geospatial index within the given radius.
        
        Args:
            key: Key to query
            longitude: Longitude of the center
            latitude: Latitude of the center
            radius: Radius to search within
            unit: Unit of distance ('m', 'km', 'mi', 'ft')
            withdist: Include distance in the results
            withcoord: Include coordinates in the results
            withhash: Include hash in the results
            count: Limit the number of results
            sort: Sort results ('ASC' or 'DESC')
            store: Store the results in the specified key
            storedist: Store the distances in the specified key
            
        Returns:
            List of members or tuples depending on the options
        """
        results = self.client.georadius(
            key, longitude, latitude, radius, unit=unit,
            withdist=withdist, withcoord=withcoord, withhash=withhash,
            count=count, sort=sort, store=store, storedist=storedist
        )
        
        if not results:
            return []
            
        # Transform the results based on the options
        if not any([withdist, withcoord, withhash]):
            # Just member names
            return [msgpack.unpackb(r) for r in results]
        
        transformed_results = []
        for result in results:
            if isinstance(result, bytes):
                # Just a member name
                transformed_results.append(msgpack.unpackb(result))
            else:
                # Tuple with optional components
                temp_result = []
                for i, item in enumerate(result):
                    if i == 0:  # The member name
                        temp_result.append(msgpack.unpackb(item))
                    else:  # One of the optional components
                        temp_result.append(item)
                transformed_results.append(tuple(temp_result))
                
        return transformed_results

    def georadiusbymember(self, key: str, member: Any, radius: float, unit: str = 'm',
                         withdist: bool = False, withcoord: bool = False, withhash: bool = False,
                         count: Optional[int] = None, sort: Optional[str] = None, store: Optional[str] = None,
                         storedist: Optional[str] = None) -> List[Any]:
        """
        Return members of a geospatial index within the given radius of a member.
        
        Args:
            key: Key to query
            member: Member to use as center
            radius: Radius to search within
            unit: Unit of distance ('m', 'km', 'mi', 'ft')
            withdist: Include distance in the results
            withcoord: Include coordinates in the results
            withhash: Include hash in the results
            count: Limit the number of results
            sort: Sort results ('ASC' or 'DESC')
            store: Store the results in the specified key
            storedist: Store the distances in the specified key
            
        Returns:
            List of members or tuples depending on the options
        """
        packed_member = msgpack.packb(member)
        results = self.client.georadiusbymember(
            key, packed_member, radius, unit=unit,
            withdist=withdist, withcoord=withcoord, withhash=withhash,
            count=count, sort=sort, store=store, storedist=storedist
        )
        
        if not results:
            return []
            
        # Transform the results based on the options
        if not any([withdist, withcoord, withhash]):
            # Just member names
            return [msgpack.unpackb(r) for r in results]
        
        transformed_results = []
        for result in results:
            if isinstance(result, bytes):
                # Just a member name
                transformed_results.append(msgpack.unpackb(result))
            else:
                # Tuple with optional components
                temp_result = []
                for i, item in enumerate(result):
                    if i == 0:  # The member name
                        temp_result.append(msgpack.unpackb(item))
                    else:  # One of the optional components
                        temp_result.append(item)
                transformed_results.append(tuple(temp_result))
                
        return transformed_results


class RedisMsgpackPipeline:
    """
    Pipeline for the RedisMsgpackClient that serializes values using MessagePack.
    """
    
    def __init__(self, pipeline, client):
        """
        Initialize the pipeline with MessagePack serialization.
        
        Args:
            pipeline: Redis pipeline
            client: Redis client with MessagePack serialization
        """
        self.pipeline = pipeline
        self.client = client
        self._stack = []

    def __getattr__(self, name):
        """
        Get an attribute from the wrapped pipeline.
        
        Args:
            name: Attribute name
            
        Returns:
            Attribute value or callable that serializes values
        """
        attr = getattr(self.pipeline, name)
        if callable(attr):
            # Return a wrapper that keeps track of commands
            return self._wrap_command(name, attr)
        return attr

    def _wrap_command(self, name, method):
        """
        Wrap a pipeline command with MessagePack serialization.
        
        Args:
            name: Command name
            method: Command method
            
        Returns:
            Wrapped command method
        """
        def wrapped(*args, **kwargs):
            # Add the command to the stack
            self._stack.append((name, args, kwargs))
            # Call the original method
            return method(*args, **kwargs)
        return wrapped

    def execute(self, raise_on_error=True):
        """
        Execute all the commands in the pipeline.
        
        Args:
            raise_on_error: Whether to raise an exception on error
            
        Returns:
            List of results
        """
        # Execute the pipeline
        results = self.pipeline.execute(raise_on_error=raise_on_error)
        
        # Process the results
        processed_results = []
        for i, result in enumerate(results):
            if i < len(self._stack):
                name, args, kwargs = self._stack[i]
                # Apply msgpack unpacking based on the command
                if name in ['get', 'hget', 'lpop', 'rpop', 'lindex', 'spop', 'srandmember'] and result is not None:
                    processed_results.append(msgpack.unpackb(result))
                elif name in ['mget', 'hmget', 'lrange', 'hvals'] and result:
                    processed_results.append([msgpack.unpackb(v) if v else None for v in result])
                elif name == 'hgetall' and result:
                    processed_results.append({k.decode(): msgpack.unpackb(v) for k, v in result.items()})
                elif name in ['smembers', 'sinter', 'sunion', 'sdiff'] and result:
                    processed_results.append({msgpack.unpackb(v) for v in result})
                elif name in ['blpop', 'brpop'] and result:
                    key, value = result
                    processed_results.append((key.decode(), msgpack.unpackb(value)))
                elif name == 'brpoplpush' and result:
                    processed_results.append(msgpack.unpackb(result))
                elif name == 'zrange' and result:
                    withscores = kwargs.get('withscores', False)
                    if withscores:
                        processed_results.append([(msgpack.unpackb(v), score) for v, score in result])
                    else:
                        processed_results.append([msgpack.unpackb(v) for v in result])
                elif name == 'zrangebyscore' and result:
                    withscores = kwargs.get('withscores', False)
                    if withscores:
                        processed_results.append([(msgpack.unpackb(v), score) for v, score in result])
                    else:
                        processed_results.append([msgpack.unpackb(v) for v in result])
                else:
                    processed_results.append(result)
            else:
                processed_results.append(result)
                
        # Clear the command stack
        self._stack = []
        
        return processed_results


class RedisMsgpackPubSub:
    """
    PubSub for the RedisMsgpackClient that serializes messages using MessagePack.
    """
    
    def __init__(self, pubsub):
        """
        Initialize the PubSub with MessagePack serialization.
        
        Args:
            pubsub: Redis PubSub
        """
        self.pubsub = pubsub

    def __getattr__(self, name):
        """
        Get an attribute from the wrapped PubSub.
        
        Args:
            name: Attribute name
            
        Returns:
            Attribute value
        """
        return getattr(self.pubsub, name)

    def get_message(self, ignore_subscribe_messages=False, timeout=0):
        """
        Get a message from the PubSub channel.
        
        Args:
            ignore_subscribe_messages: Whether to ignore subscribe/unsubscribe messages
            timeout: Timeout in seconds
            
        Returns:
            Message data or None
        """
        message = self.pubsub.get_message(ignore_subscribe_messages=ignore_subscribe_messages, timeout=timeout)
        if message and message['type'] == 'message' and 'data' in message and isinstance(message['data'], bytes):
            try:
                message['data'] = msgpack.unpackb(message['data'])
            except:
                # If unpacking fails, keep the original data
                pass
        return message

    def listen(self):
        """
        Listen for messages on the PubSub channel.
        
        Yields:
            Messages with MessagePack deserialized data
        """
        for message in self.pubsub.listen():
            if message['type'] == 'message' and 'data' in message and isinstance(message['data'], bytes):
                try:
                    message['data'] = msgpack.unpackb(message['data'])
                except:
                    # If unpacking fails, keep the original data
                    pass
            yield message

    def close(self):
        """
        Close the PubSub connection.
        """
        self.pubsub.close()
