"""FastAPI application: REST endpoints for tasks, prices and alerts."""
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response, status

from .config import Settings
from .redis_client import create_redis
from .schemas import AlertCreate, AlertOut, Notification, PricePoint, TaskCreate, TaskOut
from .tasks import TaskContext
from .worker import Worker, build_context


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings()
    redis = create_redis(settings.redis_url)
    http = httpx.AsyncClient(timeout=10)
    ctx = build_context(redis, settings, http)
    app.state.ctx = ctx
    app.state.redis = redis

    worker = None
    if settings.embedded_worker:
        worker = Worker(ctx, redis, settings)
        worker.start()
    app.state.worker = worker

    yield

    if worker:
        await worker.stop()
    await http.aclose()
    await redis.aclose()


app = FastAPI(
    title="Task Management API & Price Alert Service",
    description="Collects prices from several sources in the background and fires alerts.",
    version="1.0.0",
    lifespan=lifespan,
)


def _ctx(request: Request) -> TaskContext:
    return request.app.state.ctx


@app.get("/health", tags=["system"])
async def health(request: Request) -> dict:
    await request.app.state.redis.ping()
    return {"status": "ok"}


@app.post("/tasks", response_model=TaskOut, status_code=status.HTTP_202_ACCEPTED, tags=["tasks"])
async def create_task(body: TaskCreate, request: Request) -> dict:
    return await _ctx(request).queue.enqueue(body.type, body.payload)


@app.get("/tasks", response_model=list[TaskOut], tags=["tasks"])
async def list_tasks(request: Request, limit: int = Query(50, ge=1, le=200)) -> list[dict]:
    return await _ctx(request).queue.list(limit)


@app.get("/tasks/{task_id}", response_model=TaskOut, tags=["tasks"])
async def get_task(task_id: str, request: Request) -> dict:
    task = await _ctx(request).queue.get(task_id)
    if task is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Task not found")
    return task


@app.get("/prices", response_model=dict[str, PricePoint], tags=["prices"])
async def latest_prices(request: Request) -> dict:
    return await _ctx(request).prices.latest()


@app.get("/prices/{symbol}/history", response_model=list[PricePoint], tags=["prices"])
async def price_history(
    symbol: str, request: Request, limit: int = Query(100, ge=1, le=1000)
) -> list[dict]:
    return await _ctx(request).prices.history(symbol, limit)


@app.post("/alerts", response_model=AlertOut, status_code=status.HTTP_201_CREATED, tags=["alerts"])
async def create_alert(body: AlertCreate, request: Request) -> dict:
    return await _ctx(request).alerts.create(body.symbol, body.direction, body.target)


@app.get("/alerts", response_model=list[AlertOut], tags=["alerts"])
async def list_alerts(request: Request, active_only: bool = False) -> list[dict]:
    alerts = _ctx(request).alerts
    return await (alerts.active() if active_only else alerts.all())


@app.delete("/alerts/{alert_id}", status_code=status.HTTP_204_NO_CONTENT, tags=["alerts"])
async def delete_alert(alert_id: str, request: Request) -> Response:
    if not await _ctx(request).alerts.delete(alert_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Alert not found")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/notifications", response_model=list[Notification], tags=["alerts"])
async def list_notifications(request: Request, limit: int = Query(50, ge=1, le=500)) -> list[dict]:
    return await _ctx(request).alerts.notifications(limit)
