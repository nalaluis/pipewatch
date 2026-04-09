"""Alert deduplication: suppress identical alerts within a configurable window."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from pipewatch.alerts import Alert, AlertLevel


@dataclass
class DeduplicationConfig:
    """Configuration for alert deduplication."""
    window_seconds: int = 300  # 5 minutes default
    enabled: bool = True


@dataclass
class DeduplicationState:
    """Tracks when each unique alert was last emitted."""
    _seen: Dict[str, float] = field(default_factory=dict)

    def _key(self, alert: Alert) -> str:
        return f"{alert.pipeline}:{alert.level.value}:{alert.metric}"

    def record(self, alert: Alert) -> None:
        """Record that an alert was emitted now."""
        self._seen[self._key(alert)] = time.monotonic()

    def seconds_since(self, alert: Alert) -> Optional[float]:
        """Return seconds since this alert was last emitted, or None if never."""
        ts = self._seen.get(self._key(alert))
        if ts is None:
            return None
        return time.monotonic() - ts

    def reset(self, alert: Alert) -> None:
        """Clear the deduplication record for a specific alert."""
        self._seen.pop(self._key(alert), None)


def should_deduplicate(
    alert: Alert,
    state: DeduplicationState,
    config: DeduplicationConfig,
) -> bool:
    """Return True if the alert should be suppressed as a duplicate."""
    if not config.enabled:
        return False
    elapsed = state.seconds_since(alert)
    if elapsed is None:
        return False
    return elapsed < config.window_seconds


def filter_duplicates(
    alerts: list[Alert],
    state: DeduplicationState,
    config: DeduplicationConfig,
) -> list[Alert]:
    """Filter out duplicate alerts and record the ones that pass through."""
    passed: list[Alert] = []
    for alert in alerts:
        if not should_deduplicate(alert, state, config):
            state.record(alert)
            passed.append(alert)
    return passed
