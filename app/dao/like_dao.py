"""MySQL access. MySQL is the source of truth."""
from collections import defaultdict
from datetime import datetime, timezone

import aiomysql

from app.models.like import LikeEvent


async def get_like_count(pool: aiomysql.Pool, post_id: int) -> int | None:
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SELECT like_count FROM posts WHERE id = %s", (post_id,))
        row = await cur.fetchone()
        return None if row is None else int(row[0])


async def has_liked(pool: aiomysql.Pool, post_id: int, user_id: int) -> bool:
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT 1 FROM likes WHERE post_id = %s AND user_id = %s", (post_id, user_id)
        )
        return await cur.fetchone() is not None


async def write_events(pool: aiomysql.Pool, events: list[LikeEvent]) -> dict[int, int]:
    """Apply events in order, in one transaction. Safe to retry: count changes are
    based on rows actually inserted/deleted, so a replayed event changes nothing.
    Returns the committed like_count of every post in the batch."""
    deltas: dict[int, int] = defaultdict(int)
    async with pool.acquire() as conn:
        await conn.begin()
        try:
            async with conn.cursor() as cur:
                for e in events:
                    if e.action == "like":
                        created = datetime.fromtimestamp(e.ts_ms / 1000, tz=timezone.utc)
                        rows = await cur.execute(
                            "INSERT IGNORE INTO likes (post_id, user_id, created_at) "
                            "VALUES (%s, %s, %s)",
                            (e.post_id, e.user_id, created.replace(tzinfo=None)),
                        )
                        deltas[e.post_id] += rows
                    else:
                        rows = await cur.execute(
                            "DELETE FROM likes WHERE post_id = %s AND user_id = %s",
                            (e.post_id, e.user_id),
                        )
                        deltas[e.post_id] -= rows

                # Sorted: every worker locks post rows in the same order -> no deadlocks
                for post_id in sorted(deltas):
                    if deltas[post_id]:
                        await cur.execute(
                            "UPDATE posts SET like_count = like_count + %s WHERE id = %s",
                            (deltas[post_id], post_id),
                        )

                # Read counts inside the same transaction: exactly what we commit
                post_ids = sorted({e.post_id for e in events})
                placeholders = ", ".join(["%s"] * len(post_ids))
                await cur.execute(
                    f"SELECT id, like_count FROM posts WHERE id IN ({placeholders})", post_ids)
                counts = {int(pid): int(c) for pid, c in await cur.fetchall()}
            await conn.commit()
            return counts
        except BaseException:
            await conn.rollback()
            raise
