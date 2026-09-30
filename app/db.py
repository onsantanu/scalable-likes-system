import aiomysql
import redis.asyncio as aioredis

from app import config


async def create_mysql_pool(maxsize: int = 20) -> aiomysql.Pool:
    return await aiomysql.create_pool(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        db=config.MYSQL_DB,
        autocommit=True,
        minsize=1,
        maxsize=maxsize,
    )


def create_redis(max_connections: int = 200) -> aioredis.Redis:
    # Blocking pool: when all connections are busy, a request waits (up to 5 s) for
    # one to free up instead of failing with "Too many connections".
    pool = aioredis.BlockingConnectionPool.from_url(
        config.REDIS_URL, decode_responses=True,
        max_connections=max_connections, timeout=5,
    )
    return aioredis.Redis(connection_pool=pool)


async def require_redis_version(redis: aioredis.Redis, minimum: tuple[int, int], needed_for: str):
    version = (await redis.info("server"))["redis_version"]
    if tuple(int(p) for p in version.split(".")[:2]) < minimum:
        raise SystemExit(
            f"Redis {version} is too old: {needed_for} needs Redis "
            f"{minimum[0]}.{minimum[1]}+. Use the Redis from docker-compose.yml (7.2).")
