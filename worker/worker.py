"""Queue consumer: moves like events from the Redis streams into MySQL.

Run one or more of these:  python -m worker.worker
Each shard is owned by exactly one worker at a time (lease), so a post's
events are always applied in order.
"""
import asyncio
import logging
import math
import random
import signal
import time
import uuid

import aiomysql
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError, ResponseError

from app import config
from app.dao import like_dao
from app.db import create_mysql_pool, create_redis, require_redis_version
from app.sharding import (attempts_key, consumer_name, count_key, dead_letter_key,
                          lease_key, stream_key)
from app.validators.like_validator import parse_event

log = logging.getLogger("worker")

# Errors worth retrying as-is (DB down, timeout, deadlock, lock wait)
TEMPORARY_ERRORS = (aiomysql.OperationalError, RedisConnectionError, ConnectionError,
                    asyncio.TimeoutError)

RENEW_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return 0
"""
RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


class Worker:
    def __init__(self):
        self.id = f"worker-{uuid.uuid4().hex[:8]}"
        self.redis: aioredis.Redis | None = None
        self.pool: aiomysql.Pool | None = None
        self.shard_tasks: dict[int, asyncio.Task] = {}
        self.shard_stop: dict[int, asyncio.Event] = {}
        self.stopping = asyncio.Event()
        self.written = 0

    # ---------------------------------------------------------------- startup
    async def run(self):
        self.pool = await create_mysql_pool(maxsize=config.NUM_SHARDS + 2)
        self.redis = create_redis()
        await require_redis_version(self.redis, (6, 2), "stream trimming (XTRIM MINID)")
        self._renew = self.redis.register_script(RENEW_LUA)
        self._release = self.redis.register_script(RELEASE_LUA)
        await self._ensure_groups()
        log.info("%s started", self.id)

        background = [asyncio.create_task(self._lease_manager()),
                      asyncio.create_task(self._trimmer()),
                      asyncio.create_task(self._stats())]
        await self.stopping.wait()

        log.info("shutting down, releasing leases")
        # Stop the lease manager first, otherwise it re-acquires shards we release
        for t in background:
            t.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        await asyncio.gather(*(self._give_up_shard(s) for s in list(self.shard_tasks)))
        await self.redis.zrem("workers", self.id)
        log.info("stopped cleanly")
        await self.redis.aclose()
        self.pool.close()
        await self.pool.wait_closed()

    async def _ensure_groups(self):
        for s in range(config.NUM_SHARDS):
            try:
                # id="0": include messages appended before the group existed
                await self.redis.xgroup_create(stream_key(s), config.CONSUMER_GROUP,
                                               id="0", mkstream=True)
            except ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise

    # ---------------------------------------------------------------- leases
    async def _lease_manager(self):
        while True:
            try:
                await self._balance_leases()
            except Exception:
                log.exception("lease manager error")
            await asyncio.sleep(config.LEASE_RENEW_EVERY_SECONDS)

    async def _balance_leases(self):
        now = time.time()
        await self.redis.zadd("workers", {self.id: now})
        await self.redis.zremrangebyscore("workers", 0, now - config.LEASE_TTL_SECONDS)
        live = max(1, await self.redis.zcard("workers"))
        fair_share = math.ceil(config.NUM_SHARDS / live)

        # Renew what we own; drop shards whose lease we lost
        for s in list(self.shard_tasks):
            ok = await self._renew(keys=[lease_key(s)],
                                   args=[self.id, config.LEASE_TTL_SECONDS])
            if not ok:
                log.warning("lost lease on shard %d", s)
                await self._stop_shard_task(s)

        # A new worker joined: give back extras so it can take them
        while len(self.shard_tasks) > fair_share:
            await self._give_up_shard(next(iter(self.shard_tasks)))

        # Take free shards up to our fair share
        shards = list(range(config.NUM_SHARDS))
        random.shuffle(shards)
        for s in shards:
            if len(self.shard_tasks) >= fair_share:
                break
            if s in self.shard_tasks:
                continue
            if await self.redis.set(lease_key(s), self.id, nx=True,
                                    ex=config.LEASE_TTL_SECONDS):
                self.shard_stop[s] = asyncio.Event()
                self.shard_tasks[s] = asyncio.create_task(self._shard_loop(s))
        log.info("owns %d shards (fair share %d, live workers %d)",
                 len(self.shard_tasks), fair_share, live)

    async def _stop_shard_task(self, s: int):
        self.shard_stop[s].set()
        await self.shard_tasks[s]          # finishes its current batch first
        del self.shard_tasks[s], self.shard_stop[s]

    async def _give_up_shard(self, s: int):
        await self._stop_shard_task(s)
        await self._release(keys=[lease_key(s)], args=[self.id])

    async def _still_owner(self, s: int) -> bool:
        return await self.redis.get(lease_key(s)) == self.id

    # ---------------------------------------------------------------- consuming
    async def _shard_loop(self, s: int):
        while not self.shard_stop[s].is_set():
            try:
                await self._process_round(s)
            except Exception:
                log.exception("shard %d round failed", s)
                await asyncio.sleep(1)

    async def _read(self, s: int, from_id: str, block: int | None = None):
        resp = await self.redis.xreadgroup(
            config.CONSUMER_GROUP, consumer_name(s), {stream_key(s): from_id},
            count=config.BATCH_SIZE, block=block)
        return resp[0][1] if resp else []

    async def _process_round(self, s: int):
        # Pending first ("0"): anything read earlier but not acknowledged - from a
        # failed batch or a crashed previous owner. Only then new messages (">").
        # This keeps per-post order even across failures.
        msgs = await self._read(s, "0")
        if not msgs:
            msgs = await self._read(s, ">", block=config.READ_BLOCK_MS)
        if not msgs:
            return
        if not await self._still_owner(s):
            return                          # the new owner will process these

        try:
            events = [parse_event(mid, fields) for mid, fields in msgs]
            await self._commit(events)
            await self._ack(s, [mid for mid, _ in msgs])
            self.written += len(msgs)
        except TEMPORARY_ERRORS as e:
            log.warning("shard %d temporary error, retrying: %r", s, e)
            await asyncio.sleep(2)          # no ACK -> stays pending -> retried
        except Exception as e:
            log.warning("shard %d batch failed (%r), isolating bad message", s, e)
            await self._isolate_poison(s, msgs)

    async def _isolate_poison(self, s: int, msgs):
        """Retry one by one, in order. A message failing MAX_ATTEMPTS times goes
        to the dead-letter stream so it can't block the whole shard."""
        for mid, fields in msgs:
            try:
                await self._commit([parse_event(mid, fields)])
                await self._ack(s, [mid])
                self.written += 1
            except TEMPORARY_ERRORS:
                await asyncio.sleep(2)
                return
            except Exception as e:
                attempts = await self.redis.hincrby(attempts_key(s), mid, 1)
                if attempts >= config.MAX_ATTEMPTS:
                    log.error("shard %d: dead-lettering %s after %d attempts: %r",
                              s, mid, attempts, e)
                    await self.redis.xadd(dead_letter_key(s), {
                        **(fields or {}), "orig_id": mid, "error": repr(e)[:500]})
                    await self._ack(s, [mid])
                else:
                    await asyncio.sleep(1)
                    return                  # stop here to keep order; retry later

    async def _commit(self, events):
        """Write to MySQL, then put the committed counts in the cache.
        The worker is the only writer of cached counts; it overwrites (no NX), so a
        reader that loaded an older count from MySQL can never win. Counts are set for
        every post in the batch, even unchanged ones, so a batch replayed after a crash
        between commit and cache update still repairs the cache."""
        counts = await like_dao.write_events(self.pool, events)
        pipe = self.redis.pipeline(transaction=False)
        for post_id, count in counts.items():
            pipe.set(count_key(post_id), count, ex=config.CACHE_TTL_SECONDS)
        await pipe.execute()

    async def _ack(self, s: int, ids: list[str]):
        pipe = self.redis.pipeline(transaction=False)
        pipe.xack(stream_key(s), config.CONSUMER_GROUP, *ids)
        pipe.hdel(attempts_key(s), *ids)
        await pipe.execute()

    # ---------------------------------------------------------------- housekeeping
    async def _trimmer(self):
        while True:
            await asyncio.sleep(config.TRIM_EVERY_SECONDS)
            for s in list(self.shard_tasks):
                try:
                    await self._trim(s)
                except Exception:
                    log.exception("trim shard %d failed", s)

    async def _trim(self, s: int):
        """Delete only acknowledged messages."""
        stream = stream_key(s)
        pending = await self.redis.xpending(stream, config.CONSUMER_GROUP)
        if pending["pending"] > 0:
            min_id = pending["min"]
        else:
            groups = await self.redis.xinfo_groups(stream)
            group = next(g for g in groups if g["name"] == config.CONSUMER_GROUP)
            min_id = group["last-delivered-id"]
        # MINID removes entries with id < min_id (min_id itself is kept)
        await self.redis.xtrim(stream, minid=min_id, approximate=False)

    async def _stats(self):
        last = 0
        while True:
            await asyncio.sleep(5)
            rate = (self.written - last) / 5
            last = self.written
            if rate:
                log.info("wrote %.0f events/s (total %d)", rate, self.written)


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    worker = Worker()

    async def runner():
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, worker.stopping.set)
        await worker.run()

    asyncio.run(runner())


if __name__ == "__main__":
    main()
