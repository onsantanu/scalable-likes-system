"""Wait for the queue to drain, then check Redis and MySQL agree.

python -m scripts.verify
"""
import asyncio
import sys
import time

from app import config
from app.db import create_mysql_pool, create_redis, require_redis_version
from app.sharding import count_key, dead_letter_key, stream_key


async def queue_backlog(redis) -> tuple[int, int]:
    pending = lag = 0
    for s in range(config.NUM_SHARDS):
        for g in await redis.xinfo_groups(stream_key(s)):
            if g["name"] == config.CONSUMER_GROUP:
                pending += g["pending"]
                lag += g.get("lag") or 0
    return pending, lag


async def main():
    redis = create_redis()
    await require_redis_version(redis, (7, 0), "the queue backlog check (XINFO GROUPS lag)")
    pool = await create_mysql_pool()
    failures = 0

    # 1. Wait for workers to drain every stream
    deadline = time.time() + 120
    while True:
        pending, lag = await queue_backlog(redis)
        if pending == 0 and lag == 0:
            print("queue drained")
            break
        if time.time() > deadline:
            print(f"FAIL: queue not drained (pending={pending}, lag={lag}) - is a worker running?")
            sys.exit(1)
        print(f"waiting... pending={pending} not-yet-read={lag}")
        await asyncio.sleep(1)

    async with pool.acquire() as conn, conn.cursor() as cur:
        # 2. Stored count == actual rows in likes
        await cur.execute("""
            SELECT p.id, p.like_count, COUNT(l.user_id)
            FROM posts p LEFT JOIN likes l ON l.post_id = p.id
            GROUP BY p.id, p.like_count""")
        rows = await cur.fetchall()
        bad = [(pid, stored, actual) for pid, stored, actual in rows if stored != actual]
        print(f"[MySQL] like_count matches COUNT(*): {len(rows) - len(bad)}/{len(rows)} posts")
        for b in bad[:10]:
            print("   mismatch post=%s stored=%s actual=%s" % b)
        failures += len(bad)

        # 3. Redis cached count == MySQL count
        mysql_counts = {pid: stored for pid, stored, _ in rows}
        cached = await redis.mget([count_key(pid) for pid in mysql_counts])
        checked = bad_cache = 0
        for (pid, stored), c in zip(mysql_counts.items(), cached):
            if c is None:
                continue
            checked += 1
            if int(c) != stored:
                bad_cache += 1
                print(f"   cache mismatch post={pid} redis={c} mysql={stored}")
        print(f"[Redis] cached count matches MySQL: {checked - bad_cache}/{checked} cached posts")
        failures += bad_cache

        # 4. Redis per-user liked flag == row exists in MySQL (sample)
        keys = []
        async for k in redis.scan_iter(match="*:liked:*", count=1000):
            keys.append(k)
            if len(keys) >= 3000:
                break
        values = await redis.mget(keys) if keys else []
        bad_flag = 0
        for k, v in zip(keys, values):
            _, _, post_id, user_id = k.split(":")
            await cur.execute("SELECT 1 FROM likes WHERE post_id=%s AND user_id=%s",
                              (post_id, user_id))
            exists = await cur.fetchone() is not None
            if exists != (v == "1"):
                bad_flag += 1
        print(f"[Redis] liked flags match MySQL: {len(keys) - bad_flag}/{len(keys)} sampled")
        failures += bad_flag

    dead = sum([await redis.xlen(dead_letter_key(s)) for s in range(config.NUM_SHARDS)])
    print(f"dead-letter messages: {dead}")

    print("\nPASS" if failures == 0 else f"\nFAIL ({failures} problems)")
    await redis.aclose()
    pool.close()
    await pool.wait_closed()
    sys.exit(0 if failures == 0 else 1)


if __name__ == "__main__":
    asyncio.run(main())
