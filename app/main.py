from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from app.dao.like_cache_dao import LikeCacheDao
from app.db import create_mysql_pool, create_redis
from app.routers import likes


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await create_mysql_pool()
    redis = create_redis()
    app.state.like_cache_dao = LikeCacheDao(redis, pool)
    yield
    await redis.aclose()
    pool.close()
    await pool.wait_closed()


app = FastAPI(title="Likes demo", lifespan=lifespan)
app.include_router(likes.router)


@app.exception_handler(RedisConnectionError)
@app.exception_handler(RedisTimeoutError)
async def redis_unavailable(request: Request, exc: Exception):
    # Overloaded (no free connection within the pool timeout) or Redis down:
    # tell the client to retry, rather than reporting a server bug.
    return JSONResponse(status_code=503, content={"detail": "busy, retry shortly"},
                        headers={"Retry-After": "1"})
