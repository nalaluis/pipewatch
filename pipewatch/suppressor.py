"""Alert suppression logic — deduplicate and silence repeated alerts."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.alerts import Alert, AlertLevel


@dataclass
class SuppressionRule:
    """Defines how long (in seconds) to suppress repeated alerts for a pipeline."""

    pipeline: str
    level: AlertLevel
    cooldown_seconds: int = 300  # 5 minutes default


@dataclass
class SuppressionState:
    """Tracks the last time an alert was emitted per (pipeline, level) key."""

    _last_seen: Dict[str, float] = field(default_factory=dict)

    def _key(self, pipeline: str, level: AlertLevel) -> str:
        return f"{pipeline}::{level.value}"

    def record(self, pipeline: str, level: AlertLevel) -> None:
        self._last_seen[self._key(pipeline, level)] = time.monotonic()

    def seconds_since(self, pipeline: str, level: AlertLevel) -> Optional[float]:
        key = self._key(pipeline, level)
        if key not in self._last_seen:
            return None
        return time.monotonic() - self._last_seen[key]

    def reset(self, pipeline: str, level: AlertLevel) -> None:
        key = self._key(pipeline, level)
        self._last_seen.pop(key, None)

    def reset_all(self) -> None:
        """Clear all suppression state, allowing all alerts to be emitted again."""
        self._last_seen.clear()


def should_suppress(
    alert: Alert,
    state: SuppressionState,
    rules: List[SuppressionRule],
) -> bool:
    """Return True if the alert should be suppressed based on cooldown rules."""
    for rule in rules:
        if rule.pipeline == alert.pipeline and rule.level == alert.level:
            elapsed = state.seconds_since(alert.pipeline, alert.level)
            if elapsed is not None and elapsed < rule.cooldown_seconds:
                return True
    return False


def filter_alerts(
    alerts: List[Alert],
    state: SuppressionState,
    rules: List[SuppressionRule],
) -> List[Alert]:
    """Return only alerts that are not suppressed; update state for emitted ones."""
    allowed: List[Alert] = []
    for alert in alerts:
        if not should_suppress(alert, state, rules):
            state.record(alert.pipeline, alert.level)
            allowed.append(alert)
    return allowed
