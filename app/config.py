import os

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "password")
MYSQL_DB = os.getenv("MYSQL_DB", "likes_demo")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Must be identical for API and worker. Changing it remaps posts to different
# streams, so only change it when the streams are fully drained.
NUM_SHARDS = int(os.getenv("NUM_SHARDS", "16"))

CACHE_TTL_SECONDS = 24 * 3600
CONSUMER_GROUP = "like_writers"

LEASE_TTL_SECONDS = 30
LEASE_RENEW_EVERY_SECONDS = 10
BATCH_SIZE = 500
READ_BLOCK_MS = 1000
MAX_ATTEMPTS = 5
TRIM_EVERY_SECONDS = 60
