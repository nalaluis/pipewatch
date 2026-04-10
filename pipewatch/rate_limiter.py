"""Rate limiter for controlling how frequently pipeline checks are executed."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class RateLimiterConfig:
    """Configuration for the rate limiter."""
    min_interval_seconds: float = 60.0
    per_pipeline: bool = True


@dataclass
class RateLimiterState:
    """Tracks last execution times for rate limiting."""
    _last_run: Dict[str, float] = field(default_factory=dict)

    def record(self, key: str, ts: Optional[float] = None) -> None:
        self._last_run[key] = ts if ts is not None else time.monotonic()

    def last_run(self, key: str) -> Optional[float]:
        return self._last_run.get(key)

    def reset(self, key: str) -> None:
        self._last_run.pop(key, None)

    def reset_all(self) -> None:
        self._last_run.clear()


_GLOBAL_KEY = "__global__"


def _key(pipeline: str, per_pipeline: bool) -> str:
    return pipeline if per_pipeline else _GLOBAL_KEY


def seconds_since_last_run(
    state: RateLimiterState,
    pipeline: str,
    per_pipeline: bool = True,
) -> Optional[float]:
    """Return seconds since the last run, or None if never run."""
    k = _key(pipeline, per_pipeline)
    last = state.last_run(k)
    if last is None:
        return None
    return time.monotonic() - last


def should_run(
    state: RateLimiterState,
    config: RateLimiterConfig,
    pipeline: str,
) -> bool:
    """Return True if enough time has passed to allow another run."""
    elapsed = seconds_since_last_run(state, pipeline, config.per_pipeline)
    if elapsed is None:
        return True
    return elapsed >= config.min_interval_seconds


def record_run(
    state: RateLimiterState,
    config: RateLimiterConfig,
    pipeline: str,
    ts: Optional[float] = None,
) -> None:
    """Record that a run occurred for the given pipeline."""
    k = _key(pipeline, config.per_pipeline)
    state.record(k, ts)
