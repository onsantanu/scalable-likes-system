"""Key naming. Every key of a shard carries the hash tag {sN}, so in Redis
Cluster they all live on the same node - required for the Lua script."""
import zlib

from app.config import NUM_SHARDS


def shard_of(post_id: int) -> int:
    # crc32 is stable across processes (Python's hash() is not for strings)
    return zlib.crc32(str(post_id).encode()) % NUM_SHARDS


def _tag(shard: int) -> str:
    return "{s%d}" % shard


def count_key(post_id: int) -> str:
    return f"{_tag(shard_of(post_id))}:count:{post_id}"


def liked_key(post_id: int, user_id: int) -> str:
    return f"{_tag(shard_of(post_id))}:liked:{post_id}:{user_id}"


def stream_key(shard: int) -> str:
    return f"{_tag(shard)}:stream"


def attempts_key(shard: int) -> str:
    return f"{_tag(shard)}:attempts"


def dead_letter_key(shard: int) -> str:
    return f"{_tag(shard)}:dead"


def lease_key(shard: int) -> str:
    return f"lease:shard:{shard}"


def consumer_name(shard: int) -> str:
    # Fixed per shard, not per worker: a new owner inherits the old owner's pending messages
    return f"shard-{shard}"
