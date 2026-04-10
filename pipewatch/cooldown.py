"""Cooldown tracker: prevents re-alerting for a pipeline within a quiet window."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from pipewatch.alerts import Alert


@dataclass
class CooldownConfig:
    default_seconds: float = 300.0
    per_pipeline: Dict[str, float] = field(default_factory=dict)


@dataclass
class CooldownState:
    _last_alert: Dict[str, float] = field(default_factory=dict)

    def record(self, key: str) -> None:
        self._last_alert[key] = time.monotonic()

    def last_alert_at(self, key: str) -> Optional[float]:
        return self._last_alert.get(key)

    def reset(self, key: str) -> None:
        self._last_alert.pop(key, None)

    def seconds_since(self, key: str) -> Optional[float]:
        ts = self._last_alert.get(key)
        if ts is None:
            return None
        return time.monotonic() - ts


def _key(alert: Alert) -> str:
    return f"{alert.pipeline}::{alert.reason}"


def window_for(pipeline: str, config: CooldownConfig) -> float:
    return config.per_pipeline.get(pipeline, config.default_seconds)


def should_cooldown(alert: Alert, state: CooldownState, config: CooldownConfig) -> bool:
    """Return True if the alert is within the cooldown window and should be suppressed."""
    k = _key(alert)
    elapsed = state.seconds_since(k)
    if elapsed is None:
        return False
    return elapsed < window_for(alert.pipeline, config)


def apply_cooldown(
    alerts: list[Alert],
    state: CooldownState,
    config: CooldownConfig,
) -> list[Alert]:
    """Filter alerts that are still within their cooldown window; record the rest."""
    allowed: list[Alert] = []
    for alert in alerts:
        if should_cooldown(alert, state, config):
            continue
        state.record(_key(alert))
        allowed.append(alert)
    return allowed
