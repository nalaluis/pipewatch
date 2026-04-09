"""Rate-limiting / throttle for alert emission.

Prevents the same alert from firing more than once per cooldown window.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from pipewatch.alerts import Alert, AlertLevel


@dataclass
class ThrottleConfig:
    """Configuration for the throttle."""
    cooldown_seconds: int = 300          # 5-minute default
    min_level: AlertLevel = AlertLevel.WARNING


@dataclass
class ThrottleState:
    """Mutable state tracking last-fired timestamps keyed by (pipeline, level)."""
    _last_fired: Dict[str, float] = field(default_factory=dict)

    def _key(self, alert: Alert) -> str:
        return f"{alert.pipeline}::{alert.level.value}"

    def record(self, alert: Alert, now: Optional[float] = None) -> None:
        """Mark an alert as having fired right now."""
        self._last_fired[self._key(alert)] = now if now is not None else time.monotonic()

    def seconds_since(self, alert: Alert, now: Optional[float] = None) -> Optional[float]:
        """Return seconds since this alert last fired, or None if never fired."""
        ts = self._last_fired.get(self._key(alert))
        if ts is None:
            return None
        current = now if now is not None else time.monotonic()
        return current - ts

    def reset(self, alert: Alert) -> None:
        """Clear throttle state for a specific alert."""
        self._last_fired.pop(self._key(alert), None)


def should_throttle(
    alert: Alert,
    state: ThrottleState,
    config: ThrottleConfig,
    now: Optional[float] = None,
) -> bool:
    """Return True if the alert should be suppressed due to throttling."""
    if alert.level.value < config.min_level.value:
        return False
    elapsed = state.seconds_since(alert, now=now)
    if elapsed is None:
        return False
    return elapsed < config.cooldown_seconds


def apply_throttle(
    alerts: list[Alert],
    state: ThrottleState,
    config: ThrottleConfig,
    now: Optional[float] = None,
) -> list[Alert]:
    """Filter *alerts* through the throttle, recording newly-passed alerts."""
    passed: list[Alert] = []
    for alert in alerts:
        if should_throttle(alert, state, config, now=now):
            continue
        state.record(alert, now=now)
        passed.append(alert)
    return passed
