# Asynchronous Task Management API & Price Alert Service

A background service that collects prices from several public sources on a
schedule, stores them in Redis and fires alerts when a price crosses a target.
Work runs through a custom Redis task queue and is exposed over a REST API.

## Architecture

```
Client ──REST──> FastAPI ──enqueue──> Redis queue <──claim── Workers ──> CoinGecko
                    │                     │                     │        Frankfurter
                    └───── reads ─────> prices, alerts <─ writes┘        Simulated
                                                        ▲
                                     Scheduler ─────────┘ enqueues fetch_prices every POLL_INTERVAL
```

## Design decisions

- **No lost tasks.** `BLMOVE` atomically moves a claimed task to a processing list.
  Tasks from a crashed worker are requeued after a visibility timeout.
- **Retries with exponential backoff,** using a Redis sorted set of due times.
- **Partial failure is tolerated.** Sources are fetched concurrently, and one failing
  source does not stop the others. The task fails only if every source fails.
- **Alerts fire exactly once,** even with several workers, thanks to an atomic `HSETNX`.
- **One schedule across workers.** A `SET NX EX` lock ensures a single fetch per interval.

## Run it

With no Redis installed, the service uses in-memory `fakeredis` and runs the worker
inside the API process:

```powershell
pip install -r requirements.txt
python -m uvicorn app.main:app --port 8000
```

With real Redis and two separate workers:

```bash
docker compose up --build
```

Interactive API docs are at http://127.0.0.1:8000/docs.

## API

| Method | Path | Description |
| --- | --- | --- |
| POST | `/tasks` | Queue `fetch_prices`, `check_alerts` or `price_report` |
| GET | `/tasks`, `/tasks/{id}` | Task status, attempts and result |
| GET | `/prices` | Latest price per symbol |
| GET | `/prices/{symbol}/history` | Stored history, newest first |
| POST | `/alerts` | Create an alert: `{"symbol": "BTC-USD", "direction": "above", "target": 100000}` |
| GET | `/alerts` | All alerts, or `?active_only=true` |
| DELETE | `/alerts/{id}` | Remove an alert |
| GET | `/notifications` | Fired alerts |

Symbols: `BTC-USD`, `ETH-USD`, `SOL-USD`, `USD-TRY`, `EUR-TRY`, `DEMO-USD` (simulated).

## Configuration

Set through environment variables, as listed in `app/config.py`: `REDIS_URL`,
`EMBEDDED_WORKER`, `SOURCES`, `POLL_INTERVAL`, `WORKER_CONCURRENCY`, `MAX_ATTEMPTS`,
`RETRY_BASE_DELAY`, `VISIBILITY_TIMEOUT` and `PRICE_HISTORY_LIMIT`.

## Tests

```powershell
python -m pytest -q
```

Covers partial source failure, alert directions, single firing under concurrency,
retries, crash recovery, the full API flow and input validation. Tests run on
`fakeredis`. The Docker Compose setup with a real Redis server has not been run yet.
