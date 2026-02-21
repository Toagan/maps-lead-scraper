"""Token-bucket rate limiter for async contexts."""

from __future__ import annotations

import asyncio
import time


class TokenBucket:
    """Simple token-bucket rate limiter."""

    def __init__(self, rate: float, capacity: int | None = None):
        self.rate = rate  # tokens per second
        self.capacity = capacity or int(rate)
        self._tokens = float(self.capacity)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            self._last = now

            if self._tokens < 1:
                wait = (1 - self._tokens) / self.rate
                await asyncio.sleep(wait)
                self._tokens = 0
                self._last = time.monotonic()
            else:
                self._tokens -= 1
