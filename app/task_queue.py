"""A small reliable task queue on top of Redis.

Life of a task:
  pending -> running -> done
                     -> pending (retry after exponential backoff)
                     -> failed  (attempts exhausted)

A claimed task is moved atomically to a "processing" list (BLMOVE), so it is not
lost if a worker crashes. Tasks stuck in "running" past the visibility timeout
are put back on the queue by requeue_stale().
"""
import json
import time
import uuid

from redis.asyncio import Redis

QUEUE_KEY = "queue:pending"
PROCESSING_KEY = "queue:processing"
DELAYED_KEY = "queue:delayed"
INDEX_KEY = "tasks:index"
TASK_TTL_SECONDS = 24 * 3600
MAX_INDEXED_TASKS = 1000


def _task_key(task_id: str) -> str:
    return f"task:{task_id}"


def _decode(raw: dict) -> dict:
    return {
        "id": raw["id"],
        "type": raw["type"],
        "payload": json.loads(raw["payload"]),
        "status": raw["status"],
        "attempts": int(raw["attempts"]),
        "result": json.loads(raw["result"]) if raw.get("result") else None,
        "error": raw.get("error") or None,
        "created_at": float(raw["created_at"]),
        "updated_at": float(raw["updated_at"]),
    }


class TaskQueue:
    def __init__(
        self,
        redis: Redis,
        max_attempts: int = 3,
        retry_base_delay: float = 2.0,
        visibility_timeout: int = 60,
    ):
        self.redis = redis
        self.max_attempts = max_attempts
        self.retry_base_delay = retry_base_delay
        self.visibility_timeout = visibility_timeout

    async def enqueue(self, task_type: str, payload: dict | None = None) -> dict:
        task_id = uuid.uuid4().hex
        now = time.time()
        task = {
            "id": task_id,
            "type": task_type,
            "payload": json.dumps(payload or {}),
            "status": "pending",
            "attempts": 0,
            "created_at": now,
            "updated_at": now,
        }
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(_task_key(task_id), mapping=task)
            pipe.zadd(INDEX_KEY, {task_id: now})
            pipe.zremrangebyrank(INDEX_KEY, 0, -MAX_INDEXED_TASKS - 1)
            pipe.lpush(QUEUE_KEY, task_id)
            await pipe.execute()
        return _decode({k: str(v) for k, v in task.items()})

    async def claim(self, timeout: float = 1.0) -> dict | None:
        task_id = await self.redis.blmove(QUEUE_KEY, PROCESSING_KEY, timeout, "RIGHT", "LEFT")
        if task_id is None:
            return None
        key = _task_key(task_id)
        if not await self.redis.exists(key):
            await self.redis.lrem(PROCESSING_KEY, 1, task_id)
            return None
        await self.redis.hset(key, mapping={"status": "running", "updated_at": time.time()})
        await self.redis.hincrby(key, "attempts", 1)
        return await self.get(task_id)

    async def complete(self, task_id: str, result: dict) -> None:
        key = _task_key(task_id)
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(
                key,
                mapping={
                    "status": "done",
                    "result": json.dumps(result),
                    "error": "",
                    "updated_at": time.time(),
                },
            )
            pipe.expire(key, TASK_TTL_SECONDS)
            pipe.lrem(PROCESSING_KEY, 1, task_id)
            await pipe.execute()

    async def fail(self, task_id: str, error: str) -> None:
        task = await self.get(task_id)
        if task is None:
            await self.redis.lrem(PROCESSING_KEY, 1, task_id)
            return
        key = _task_key(task_id)
        retry = task["attempts"] < self.max_attempts
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(
                key,
                mapping={
                    "status": "pending" if retry else "failed",
                    "error": error,
                    "updated_at": time.time(),
                },
            )
            if retry:
                delay = self.retry_base_delay * 2 ** (task["attempts"] - 1)
                pipe.zadd(DELAYED_KEY, {task_id: time.time() + delay})
            else:
                pipe.expire(key, TASK_TTL_SECONDS)
            pipe.lrem(PROCESSING_KEY, 1, task_id)
            await pipe.execute()

    async def promote_due(self) -> int:
        """Moves retries whose backoff has elapsed back onto the queue."""
        due = await self.redis.zrangebyscore(DELAYED_KEY, 0, time.time())
        promoted = 0
        for task_id in due:
            # zrem returns 0 if another worker already promoted this task.
            if await self.redis.zrem(DELAYED_KEY, task_id):
                await self.redis.lpush(QUEUE_KEY, task_id)
                promoted += 1
        return promoted

    async def requeue_stale(self) -> int:
        """Recovers tasks from workers that died mid-run."""
        requeued = 0
        cutoff = time.time() - self.visibility_timeout
        for task_id in await self.redis.lrange(PROCESSING_KEY, 0, -1):
            task = await self.get(task_id)
            if task is None or task["updated_at"] < cutoff:
                # lrem returns 0 if another worker already recovered this task.
                if await self.redis.lrem(PROCESSING_KEY, 1, task_id) and task is not None:
                    await self.redis.hset(
                        _task_key(task_id),
                        mapping={"status": "pending", "updated_at": time.time()},
                    )
                    await self.redis.lpush(QUEUE_KEY, task_id)
                    requeued += 1
        return requeued

    async def get(self, task_id: str) -> dict | None:
        raw = await self.redis.hgetall(_task_key(task_id))
        return _decode(raw) if raw else None

    async def list(self, limit: int = 50) -> list[dict]:
        task_ids = await self.redis.zrevrange(INDEX_KEY, 0, limit - 1)
        tasks = [await self.get(task_id) for task_id in task_ids]
        return [task for task in tasks if task is not None]
