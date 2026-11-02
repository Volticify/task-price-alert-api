"""Creates the async Redis client (real server or in-memory fake)."""
from redis.asyncio import Redis


def create_redis(url: str) -> Redis:
    if url.startswith("fakeredis://"):
        from fakeredis import FakeAsyncRedis

        return FakeAsyncRedis(decode_responses=True)
    return Redis.from_url(url, decode_responses=True)
