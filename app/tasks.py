"""Task handlers run by the worker."""
import asyncio
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable

import httpx

from .sources import PriceSource
from .store import AlertStore, PriceStore
from .task_queue import TaskQueue

logger = logging.getLogger(__name__)


@dataclass
class TaskContext:
    queue: TaskQueue
    prices: PriceStore
    alerts: AlertStore
    sources: list[PriceSource]
    http: httpx.AsyncClient


async def fetch_prices(ctx: TaskContext, payload: dict) -> dict:
    """Pulls every source concurrently. One failing source does not stop the others."""
    results = await asyncio.gather(
        *(source.fetch(ctx.http) for source in ctx.sources), return_exceptions=True
    )
    fetched: dict[str, int] = {}
    errors: dict[str, str] = {}
    for source, result in zip(ctx.sources, results):
        if isinstance(result, Exception):
            logger.warning("Source %s failed: %s", source.name, result)
            errors[source.name] = str(result) or type(result).__name__
            continue
        await ctx.prices.save(source.name, result)
        fetched[source.name] = len(result)

    if not fetched:
        raise RuntimeError(f"All sources failed: {errors}")

    await ctx.queue.enqueue("check_alerts")
    return {"fetched": fetched, "errors": errors}


async def check_alerts(ctx: TaskContext, payload: dict) -> dict:
    latest = await ctx.prices.latest()
    triggered = []
    for alert in await ctx.alerts.active():
        point = latest.get(alert["symbol"])
        if point is None:
            continue
        price = point["price"]
        hit = price >= alert["target"] if alert["direction"] == "above" else price <= alert["target"]
        if hit and await ctx.alerts.trigger(alert, price):
            triggered.append(alert["id"])
    return {"triggered": triggered}


async def price_report(ctx: TaskContext, payload: dict) -> dict:
    """Summarises a symbol's stored history."""
    symbol = payload["symbol"]
    points = await ctx.prices.history(symbol, limit=int(payload.get("limit", 100)))
    if not points:
        raise ValueError(f"No price history for {symbol}")
    values = [point["price"] for point in points]
    first, last = values[-1], values[0]
    return {
        "symbol": symbol,
        "samples": len(values),
        "min": min(values),
        "max": max(values),
        "average": round(sum(values) / len(values), 6),
        "change_percent": round((last - first) / first * 100, 4),
    }


HANDLERS: dict[str, Callable[[TaskContext, dict], Awaitable[dict]]] = {
    "fetch_prices": fetch_prices,
    "check_alerts": check_alerts,
    "price_report": price_report,
}
