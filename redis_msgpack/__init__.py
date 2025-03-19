"""
Redis client with MessagePack serialization for efficient data storage and retrieval.
"""

from .client import RedisMsgpackClient, RedisMsgpackPipeline, RedisMsgpackPubSub

__version__ = "0.1.0"
__all__ = ["RedisMsgpackClient", "RedisMsgpackPipeline", "RedisMsgpackPubSub"]