"""Wipe likes, zero counts, flush Redis. Stop workers first.  python -m scripts.reset"""
import asyncio

from app.db import create_mysql_pool, create_redis


async def main():
    redis = create_redis()
    pool = await create_mysql_pool()
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("DELETE FROM likes")
        await cur.execute("UPDATE posts SET like_count = 0")
    await redis.flushdb()
    print("reset done")
    await redis.aclose()
    pool.close()
    await pool.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
