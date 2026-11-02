"""Request and response models for the API."""
from typing import Literal

from pydantic import BaseModel, Field

TaskType = Literal["fetch_prices", "check_alerts", "price_report"]


class TaskCreate(BaseModel):
    type: TaskType
    payload: dict = Field(default_factory=dict)


class TaskOut(BaseModel):
    id: str
    type: str
    payload: dict
    status: Literal["pending", "running", "done", "failed"]
    attempts: int
    result: dict | None = None
    error: str | None = None
    created_at: float
    updated_at: float


class PricePoint(BaseModel):
    symbol: str
    price: float
    source: str
    timestamp: float


class AlertCreate(BaseModel):
    symbol: str = Field(min_length=1, max_length=20, examples=["BTC-USD"])
    direction: Literal["above", "below"]
    target: float = Field(gt=0)


class AlertOut(BaseModel):
    id: str
    symbol: str
    direction: Literal["above", "below"]
    target: float
    created_at: float
    triggered_at: float | None = None
    triggered_price: float | None = None


class Notification(BaseModel):
    alert_id: str
    symbol: str
    message: str
    price: float
    timestamp: float
