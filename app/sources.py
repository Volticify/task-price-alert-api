"""Price data sources. Each one returns {symbol: price} in a common format."""
import asyncio
import random
from typing import Protocol

import httpx


class PriceSource(Protocol):
    name: str

    async def fetch(self, client: httpx.AsyncClient) -> dict[str, float]: ...


class CoinGeckoSource:
    """Crypto prices from the public CoinGecko API."""

    name = "coingecko"
    url = "https://api.coingecko.com/api/v3/simple/price"
    coins = {"bitcoin": "BTC-USD", "ethereum": "ETH-USD", "solana": "SOL-USD"}

    async def fetch(self, client: httpx.AsyncClient) -> dict[str, float]:
        response = await client.get(
            self.url, params={"ids": ",".join(self.coins), "vs_currencies": "usd"}
        )
        response.raise_for_status()
        data = response.json()
        return {self.coins[coin]: float(data[coin]["usd"]) for coin in self.coins if coin in data}


class FrankfurterSource:
    """Exchange rates from the public Frankfurter API."""

    name = "frankfurter"
    url = "https://api.frankfurter.dev/v1/latest"
    pairs = [("USD", "TRY"), ("EUR", "TRY")]

    async def fetch(self, client: httpx.AsyncClient) -> dict[str, float]:
        async def fetch_pair(base: str, quote: str) -> tuple[str, float]:
            response = await client.get(self.url, params={"base": base, "symbols": quote})
            response.raise_for_status()
            return f"{base}-{quote}", float(response.json()["rates"][quote])

        return dict(await asyncio.gather(*(fetch_pair(*pair) for pair in self.pairs)))


class SimulatedSource:
    """Offline random walk, so alerts can be demoed without internet access."""

    name = "simulated"

    def __init__(self, start: float = 100.0, volatility: float = 0.01):
        self.price = start
        self.volatility = volatility

    async def fetch(self, client: httpx.AsyncClient) -> dict[str, float]:
        self.price = round(self.price * (1 + random.gauss(0, self.volatility)), 4)
        return {"DEMO-USD": self.price}


AVAILABLE_SOURCES = {
    CoinGeckoSource.name: CoinGeckoSource,
    FrankfurterSource.name: FrankfurterSource,
    SimulatedSource.name: SimulatedSource,
}


def build_sources(names: list[str]) -> list[PriceSource]:
    unknown = set(names) - set(AVAILABLE_SOURCES)
    if unknown:
        raise ValueError(f"Unknown price sources: {sorted(unknown)}")
    return [AVAILABLE_SOURCES[name]() for name in names]
