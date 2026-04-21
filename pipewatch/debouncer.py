"""Debouncer: suppress alerts that resolve before a minimum persistence window."""
from __future__ import annotations

from dataclasses import dataclass, field
from time import time
from typing import Dict, Optional

from pipewatch.alerts import Alert, AlertLevel


@dataclass
class DebouncerConfig:
    min_duration_seconds: float = 30.0
    enabled: bool = True


@dataclass
class DebouncerEntry:
    first_seen: float
    level: AlertLevel
    count: int = 1

    def age_seconds(self, now: Optional[float] = None) -> float:
        return (now or time()) - self.first_seen


_state: Dict[str, DebouncerEntry] = {}


def _key(alert: Alert) -> str:
    return f"{alert.pipeline}:{alert.metric}"


def record(alert: Alert, now: Optional[float] = None) -> DebouncerEntry:
    """Record an alert occurrence; return the current entry."""
    k = _key(alert)
    ts = now or time()
    if k in _state:
        entry = _state[k]
        entry.count += 1
        entry.level = alert.level
    else:
        _state[k] = DebouncerEntry(first_seen=ts, level=alert.level)
    return _state[k]


def resolve(alert: Alert) -> None:
    """Remove an alert from the debounce window (it has cleared)."""
    _state.pop(_key(alert), None)


def should_debounce(alert: Alert, cfg: DebouncerConfig, now: Optional[float] = None) -> bool:
    """Return True if the alert has NOT persisted long enough to fire."""
    if not cfg.enabled:
        return False
    entry = _state.get(_key(alert))
    if entry is None:
        return True  # first sighting — hold it back
    return entry.age_seconds(now) < cfg.min_duration_seconds


def apply_debounce(
    alerts: list[Alert],
    cfg: DebouncerConfig,
    now: Optional[float] = None,
) -> list[Alert]:
    """Return only alerts that have persisted beyond the minimum window."""
    passed: list[Alert] = []
    ts = now or time()
    for alert in alerts:
        record(alert, ts)
        if not should_debounce(alert, cfg, ts):
            passed.append(alert)
    return passed
