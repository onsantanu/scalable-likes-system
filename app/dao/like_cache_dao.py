"""Redis access: the atomic toggle script and cache loading."""
import asyncio
import time
from pathlib import Path

import aiomysql
import redis.asyncio as aioredis

from app import config
from app.dao import like_dao
from app.sharding import count_key, liked_key, shard_of, stream_key

_LUA = (Path(__file__).parent.parent / "lua" / "toggle_like.lua").read_text()


class PostNotFound(Exception):
    pass


class LikeCacheDao:
    def __init__(self, redis: aioredis.Redis, pool: aiomysql.Pool):
        self.redis = redis
        self.pool = pool
        self._toggle = redis.register_script(_LUA)
        self._loading: dict[int, asyncio.Future] = {}   # post_id -> load in progress

    async def set_like(self, post_id: int, user_id: int, action: str) -> str:
        """Returns "OK" (event queued) or "NOOP" (already in that state).
        Touches MySQL only when the post's count isn't cached yet: once per post."""
        keys = [liked_key(post_id, user_id), count_key(post_id), stream_key(shard_of(post_id))]
        args = [user_id, post_id, action, int(time.time() * 1000), config.CACHE_TTL_SECONDS]

        result = await self._toggle(keys=keys, args=args)
        if result == "NEED_LOAD":
            await self.load_count(post_id)
            result = await self._toggle(keys=keys, args=args)
        if result == "NEED_LOAD":
            raise RuntimeError("cache not loaded, retry")
        return result

    async def get_info(self, post_id: int, user_id: int) -> tuple[int, bool]:
        count, liked = await self.redis.mget(count_key(post_id), liked_key(post_id, user_id))
        if count is None:
            count = await self.load_count(post_id)
        if liked is None:
            liked = "1" if await like_dao.has_liked(self.pool, post_id, user_id) else "0"
            await self.redis.set(liked_key(post_id, user_id), liked,
                                 ex=config.CACHE_TTL_SECONDS, nx=True)
        return int(count), liked == "1"

    async def load_count(self, post_id: int) -> int:
        """Single-flight: concurrent misses for the same post share one MySQL query,
        so a viral post whose cache expired doesn't send a stampede to MySQL."""
        task = self._loading.get(post_id)
        if task is None:
            task = asyncio.ensure_future(self._load_count(post_id))
            self._loading[post_id] = task
            task.add_done_callback(lambda _: self._loading.pop(post_id, None))
        # shield: one waiter's request being cancelled must not cancel the shared load
        return await asyncio.shield(task)

    async def _load_count(self, post_id: int) -> int:
        count = await like_dao.get_like_count(self.pool, post_id)
        if count is None:
            raise PostNotFound(post_id)
        # nx=True: if the worker already wrote a fresher count, keep it
        if not await self.redis.set(count_key(post_id), count,
                                    ex=config.CACHE_TTL_SECONDS, nx=True):
            count = int(await self.redis.get(count_key(post_id)) or count)
        return count
