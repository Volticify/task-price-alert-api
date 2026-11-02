import asyncio
import time

import httpx
import pytest
from fakeredis import FakeAsyncRedis
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import app
from app.store import AlertStore, PriceStore
from app.task_queue import TaskQueue
from app.tasks import TaskContext, check_alerts, fetch_prices
from app.worker import Worker


class FixedSource:
    name = "fixed"

    def __init__(self, prices):
        self.prices = prices

    async def fetch(self, client):
        return self.prices


class BrokenSource:
    name = "broken"

    async def fetch(self, client):
        raise httpx.ConnectError("source is down")


def make_ctx(sources):
    redis = FakeAsyncRedis(decode_responses=True)
    return TaskContext(
        queue=TaskQueue(redis, max_attempts=2, retry_base_delay=0),
        prices=PriceStore(redis),
        alerts=AlertStore(redis),
        sources=sources,
        http=None,
    )


def test_one_failing_source_does_not_stop_the_others():
    async def scenario():
        ctx = make_ctx([FixedSource({"BTC-USD": 100.0}), BrokenSource()])
        result = await fetch_prices(ctx, {})
        assert result["fetched"] == {"fixed": 1}
        assert "broken" in result["errors"]
        assert (await ctx.prices.latest())["BTC-USD"]["price"] == 100.0

    asyncio.run(scenario())


def test_task_raises_when_every_source_fails():
    async def scenario():
        ctx = make_ctx([BrokenSource()])
        with pytest.raises(RuntimeError):
            await fetch_prices(ctx, {})

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "direction,target,fires",
    [("above", 90, True), ("above", 110, False), ("below", 110, True), ("below", 90, False)],
)
def test_alert_direction(direction, target, fires):
    async def scenario():
        ctx = make_ctx([])
        await ctx.prices.save("fixed", {"BTC-USD": 100.0})
        alert = await ctx.alerts.create("BTC-USD", direction, target)
        result = await check_alerts(ctx, {})
        assert (alert["id"] in result["triggered"]) is fires

    asyncio.run(scenario())


def test_alert_fires_only_once():
    async def scenario():
        ctx = make_ctx([])
        await ctx.prices.save("fixed", {"BTC-USD": 100.0})
        alert = await ctx.alerts.create("BTC-USD", "above", 90)
        results = await asyncio.gather(
            ctx.alerts.trigger(alert, 100.0), ctx.alerts.trigger(alert, 100.0)
        )
        assert sorted(results) == [False, True]
        assert len(await ctx.alerts.notifications()) == 1
        assert await ctx.alerts.active() == []

    asyncio.run(scenario())


def test_failed_task_is_retried_then_marked_failed():
    async def scenario():
        ctx = make_ctx([BrokenSource()])
        worker = Worker(ctx, ctx.queue.redis, Settings())
        task = await ctx.queue.enqueue("fetch_prices")

        await worker.run_once()
        first = await ctx.queue.get(task["id"])
        assert first["status"] == "pending"
        assert first["attempts"] == 1

        await asyncio.sleep(0.01)
        assert await ctx.queue.promote_due() == 1
        await worker.run_once()
        second = await ctx.queue.get(task["id"])
        assert second["status"] == "failed"
        assert second["attempts"] == 2
        assert "All sources failed" in second["error"]

    asyncio.run(scenario())


def test_task_from_crashed_worker_is_recovered():
    async def scenario():
        ctx = make_ctx([])
        ctx.queue.visibility_timeout = -1
        task = await ctx.queue.enqueue("check_alerts")
        await ctx.queue.claim()  # The worker "dies" here and never completes the task.

        assert await ctx.queue.requeue_stale() == 1
        reclaimed = await ctx.queue.claim()
        assert reclaimed["id"] == task["id"]
        assert reclaimed["attempts"] == 2

    asyncio.run(scenario())


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("SOURCES", "simulated")
    monkeypatch.setenv("POLL_INTERVAL", "3600")
    with TestClient(app) as test_client:
        yield test_client


def wait_for_task(client, task_id, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        task = client.get(f"/tasks/{task_id}").json()
        if task["status"] in ("done", "failed"):
            return task
        time.sleep(0.02)
    raise AssertionError(f"Task {task_id} did not finish")


def test_end_to_end_alert_flow(client):
    task = client.post("/tasks", json={"type": "fetch_prices"})
    assert task.status_code == 202
    assert wait_for_task(client, task.json()["id"])["status"] == "done"

    prices = client.get("/prices").json()
    assert "DEMO-USD" in prices

    alert = client.post(
        "/alerts", json={"symbol": "DEMO-USD", "direction": "above", "target": 0.01}
    ).json()
    check = client.post("/tasks", json={"type": "check_alerts"}).json()
    assert alert["id"] in wait_for_task(client, check["id"])["result"]["triggered"]

    assert client.get("/alerts", params={"active_only": True}).json() == []
    notifications = client.get("/notifications").json()
    assert notifications[0]["alert_id"] == alert["id"]


def test_price_report(client):
    for _ in range(3):
        task = client.post("/tasks", json={"type": "fetch_prices"}).json()
        wait_for_task(client, task["id"])

    report = client.post(
        "/tasks", json={"type": "price_report", "payload": {"symbol": "DEMO-USD"}}
    ).json()
    result = wait_for_task(client, report["id"])["result"]
    assert result["symbol"] == "DEMO-USD"
    assert result["samples"] >= 3
    assert result["min"] <= result["average"] <= result["max"]


def test_validation_and_not_found(client):
    assert client.post("/tasks", json={"type": "delete_everything"}).status_code == 422
    assert (
        client.post("/alerts", json={"symbol": "X", "direction": "up", "target": 1}).status_code
        == 422
    )
    assert (
        client.post("/alerts", json={"symbol": "X", "direction": "above", "target": -1}).status_code
        == 422
    )
    assert client.get("/tasks/missing").status_code == 404
    assert client.delete("/alerts/missing").status_code == 404
