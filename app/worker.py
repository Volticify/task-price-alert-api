"""Background worker: consumes the queue and schedules periodic price pulls.

Run standalone with `python -m app.worker`, or embedded in the API process.
"""
import asyncio
import logging

import httpx
from redis.asyncio import Redis

from .config import Settings
from .redis_client import create_redis
from .sources import build_sources
from .store import AlertStore, PriceStore
from .task_queue import TaskQueue
from .tasks import HANDLERS, TaskContext

logger = logging.getLogger(__name__)

SCHEDULER_LOCK_KEY = "scheduler:fetch_prices"
IDLE_SLEEP_SECONDS = 0.05


class Worker:
    def __init__(self, ctx: TaskContext, redis: Redis, settings: Settings):
        self.ctx = ctx
        self.redis = redis
        self.settings = settings
        self._stopping = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    async def run_once(self, timeout: float = 1.0) -> bool:
        """Claims and runs one task. Returns False if the queue was empty."""
        task = await self.ctx.queue.claim(timeout=timeout)
        if task is None:
            return False
        handler = HANDLERS.get(task["type"])
        try:
            if handler is None:
                raise ValueError(f"Unknown task type: {task['type']}")
            result = await handler(self.ctx, task["payload"])
        except Exception as exc:
            logger.warning("Task %s (%s) failed: %s", task["id"], task["type"], exc)
            await self.ctx.queue.fail(task["id"], f"{type(exc).__name__}: {exc}")
        else:
            await self.ctx.queue.complete(task["id"], result)
        return True

    async def _consume(self) -> None:
        while not self._stopping.is_set():
            try:
                if not await self.run_once():
                    # In-memory Redis returns from a blocking pop at once, so
                    # always yield or this loop would starve the API.
                    await asyncio.sleep(IDLE_SLEEP_SECONDS)
            except Exception:
                logger.exception("Worker loop error")
                await asyncio.sleep(1)

    async def _schedule(self) -> None:
        while not self._stopping.is_set():
            try:
                # The lock's TTL is the poll interval, so that any number of
                # workers enqueue exactly one fetch per interval.
                if await self.redis.set(
                    SCHEDULER_LOCK_KEY, "1", nx=True, ex=self.settings.poll_interval
                ):
                    await self.ctx.queue.enqueue("fetch_prices")
                await self.ctx.queue.promote_due()
                await self.ctx.queue.requeue_stale()
            except Exception:
                logger.exception("Scheduler error")
            await asyncio.sleep(1)

    def start(self) -> None:
        self._tasks = [
            asyncio.create_task(self._consume())
            for _ in range(self.settings.worker_concurrency)
        ]
        self._tasks.append(asyncio.create_task(self._schedule()))

    async def stop(self) -> None:
        self._stopping.set()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def wait(self) -> None:
        await asyncio.gather(*self._tasks)


def build_context(redis: Redis, settings: Settings, http: httpx.AsyncClient) -> TaskContext:
    return TaskContext(
        queue=TaskQueue(
            redis,
            max_attempts=settings.max_attempts,
            retry_base_delay=settings.retry_base_delay,
            visibility_timeout=settings.visibility_timeout,
        ),
        prices=PriceStore(redis, history_limit=settings.price_history_limit),
        alerts=AlertStore(redis),
        sources=build_sources(settings.sources),
        http=http,
    )


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings()
    redis = create_redis(settings.redis_url)
    async with httpx.AsyncClient(timeout=10) as http:
        worker = Worker(build_context(redis, settings, http), redis, settings)
        worker.start()
        logger.info("Worker started (%d consumers)", settings.worker_concurrency)
        await worker.wait()


if __name__ == "__main__":
    asyncio.run(main())
