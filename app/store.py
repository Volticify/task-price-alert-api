"""Redis persistence for prices, alerts and notifications."""
import json
import time
import uuid

from redis.asyncio import Redis

LATEST_KEY = "prices:latest"
ALERTS_KEY = "alerts:active"
ALERT_HISTORY_KEY = "alerts:all"
NOTIFICATIONS_KEY = "notifications"
MAX_NOTIFICATIONS = 500


def _history_key(symbol: str) -> str:
    return f"prices:history:{symbol}"


def _alert_key(alert_id: str) -> str:
    return f"alert:{alert_id}"


class PriceStore:
    def __init__(self, redis: Redis, history_limit: int = 1000):
        self.redis = redis
        self.history_limit = history_limit

    async def save(self, source: str, prices: dict[str, float]) -> None:
        now = time.time()
        async with self.redis.pipeline(transaction=True) as pipe:
            for symbol, price in prices.items():
                point = {"symbol": symbol, "price": price, "source": source, "timestamp": now}
                pipe.hset(LATEST_KEY, symbol, json.dumps(point))
                pipe.zadd(_history_key(symbol), {json.dumps(point): now})
                pipe.zremrangebyrank(_history_key(symbol), 0, -self.history_limit - 1)
            await pipe.execute()

    async def latest(self) -> dict[str, dict]:
        raw = await self.redis.hgetall(LATEST_KEY)
        return {symbol: json.loads(point) for symbol, point in raw.items()}

    async def history(self, symbol: str, limit: int = 100) -> list[dict]:
        """Newest point first."""
        points = await self.redis.zrevrange(_history_key(symbol), 0, limit - 1)
        return [json.loads(point) for point in points]


class AlertStore:
    def __init__(self, redis: Redis):
        self.redis = redis

    async def create(self, symbol: str, direction: str, target: float) -> dict:
        alert_id = uuid.uuid4().hex
        alert = {
            "id": alert_id,
            "symbol": symbol,
            "direction": direction,
            "target": target,
            "created_at": time.time(),
        }
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(_alert_key(alert_id), mapping=alert)
            pipe.sadd(ALERTS_KEY, alert_id)
            pipe.sadd(ALERT_HISTORY_KEY, alert_id)
            await pipe.execute()
        return await self.get(alert_id)

    async def get(self, alert_id: str) -> dict | None:
        raw = await self.redis.hgetall(_alert_key(alert_id))
        if not raw:
            return None
        return {
            "id": raw["id"],
            "symbol": raw["symbol"],
            "direction": raw["direction"],
            "target": float(raw["target"]),
            "created_at": float(raw["created_at"]),
            "triggered_at": float(raw["triggered_at"]) if raw.get("triggered_at") else None,
            "triggered_price": float(raw["triggered_price"]) if raw.get("triggered_price") else None,
        }

    async def _load(self, key: str) -> list[dict]:
        alerts = [await self.get(alert_id) for alert_id in await self.redis.smembers(key)]
        return sorted(
            (alert for alert in alerts if alert is not None), key=lambda alert: alert["created_at"]
        )

    async def active(self) -> list[dict]:
        return await self._load(ALERTS_KEY)

    async def all(self) -> list[dict]:
        return await self._load(ALERT_HISTORY_KEY)

    async def delete(self, alert_id: str) -> bool:
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.delete(_alert_key(alert_id))
            pipe.srem(ALERTS_KEY, alert_id)
            pipe.srem(ALERT_HISTORY_KEY, alert_id)
            deleted, *_ = await pipe.execute()
        return bool(deleted)

    async def trigger(self, alert: dict, price: float) -> bool:
        """Fires an alert once. Returns False if another worker got there first."""
        key = _alert_key(alert["id"])
        if not await self.redis.hsetnx(key, "triggered_at", time.time()):
            return False
        notification = {
            "alert_id": alert["id"],
            "symbol": alert["symbol"],
            "message": (
                f"{alert['symbol']} is {price} ({alert['direction']} target {alert['target']})"
            ),
            "price": price,
            "timestamp": time.time(),
        }
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(key, "triggered_price", price)
            pipe.srem(ALERTS_KEY, alert["id"])
            pipe.lpush(NOTIFICATIONS_KEY, json.dumps(notification))
            pipe.ltrim(NOTIFICATIONS_KEY, 0, MAX_NOTIFICATIONS - 1)
            await pipe.execute()
        return True

    async def notifications(self, limit: int = 50) -> list[dict]:
        raw = await self.redis.lrange(NOTIFICATIONS_KEY, 0, limit - 1)
        return [json.loads(item) for item in raw]
