"""Nightly safety net.  python -m scripts.reconcile
1. Recompute like_count from the likes table where it drifted.
2. Drop any cached count that disagrees with MySQL, so it reloads fresh
   (e.g. after Redis restored an old snapshot).
Run it when the queue is drained: pending events make Redis legitimately ahead of MySQL."""
import asyncio

from app.db import create_mysql_pool, create_redis
from app.sharding import count_key


async def main():
    redis = create_redis()
    pool = await create_mysql_pool()
    fixed_mysql = fixed_cache = 0
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("""
            SELECT p.id, p.like_count, COUNT(l.user_id)
            FROM posts p LEFT JOIN likes l ON l.post_id = p.id
            GROUP BY p.id, p.like_count""")
        for post_id, stored, actual in await cur.fetchall():
            if stored != actual:
                await cur.execute("UPDATE posts SET like_count = %s WHERE id = %s",
                                  (actual, post_id))
                fixed_mysql += 1
            cached = await redis.get(count_key(post_id))
            if cached is not None and int(cached) != actual:
                await redis.delete(count_key(post_id))
                fixed_cache += 1
    print(f"fixed {fixed_mysql} MySQL counts, dropped {fixed_cache} stale cached counts")
    await redis.aclose()
    pool.close()
    await pool.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
