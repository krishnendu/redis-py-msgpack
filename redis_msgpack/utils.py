"""
Utility classes and functions for redis-msgpack.
"""
import msgpack
from typing import Any, Optional


class MsgpackSerializer:
    """
    MessagePack serializer for Redis data.
    
    This serializer uses MessagePack for efficient binary serialization of Python objects.
    It handles basic Python types and can be extended to handle custom types.
    """
    
    def __init__(self, **options):
        """
        Initialize the serializer with MessagePack options.
        
        Args:
            **options: Additional options to pass to msgpack pack/unpack
        """
        self.pack_options = options.get('pack_options', {})
        self.unpack_options = options.get('unpack_options', {})
    
    def dumps(self, obj: Any) -> bytes:
        """
        Serialize an object to MessagePack format.
        
        Args:
            obj: The Python object to serialize
            
        Returns:
            bytes: The serialized data
        """
        try:
            return msgpack.packb(obj, use_bin_type=True, **self.pack_options)
        except Exception as e:
            raise ValueError(f"Failed to serialize object: {e}")
    
    def loads(self, data: Optional[bytes]) -> Any:
        """
        Deserialize MessagePack data to a Python object.
        
        Args:
            data: The MessagePack data to deserialize
            
        Returns:
            The deserialized Python object
            
        Raises:
            ValueError: If deserialization fails
        """
        if data is None:
            return None
            
        try:
            return msgpack.unpackb(data, raw=False, **self.unpack_options)
        except Exception as e:
            raise ValueError(f"Failed to deserialize data: {e}") 