"""Runtime configuration, read from environment variables."""
import os
from dataclasses import dataclass, field


def _env_list(name: str, default: str) -> list[str]:
    return [item.strip() for item in os.getenv(name, default).split(",") if item.strip()]


@dataclass(frozen=True)
class Settings:
    # "fakeredis://" runs an in-memory Redis inside the process, so the project
    # works with no Redis installed. Use redis://host:6379/0 in production.
    redis_url: str = field(default_factory=lambda: os.getenv("REDIS_URL", "fakeredis://"))
    # In-memory Redis cannot be shared between processes, so the worker must run
    # inside the API process in that mode.
    embedded_worker: bool = field(
        default_factory=lambda: os.getenv("EMBEDDED_WORKER", "true").lower() == "true"
    )
    sources: list[str] = field(
        default_factory=lambda: _env_list("SOURCES", "coingecko,frankfurter,simulated")
    )
    poll_interval: int = field(default_factory=lambda: int(os.getenv("POLL_INTERVAL", "60")))
    worker_concurrency: int = field(default_factory=lambda: int(os.getenv("WORKER_CONCURRENCY", "3")))
    max_attempts: int = field(default_factory=lambda: int(os.getenv("MAX_ATTEMPTS", "3")))
    retry_base_delay: float = field(default_factory=lambda: float(os.getenv("RETRY_BASE_DELAY", "2")))
    visibility_timeout: int = field(default_factory=lambda: int(os.getenv("VISIBILITY_TIMEOUT", "60")))
    price_history_limit: int = field(default_factory=lambda: int(os.getenv("PRICE_HISTORY_LIMIT", "1000")))
