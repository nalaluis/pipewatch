"""Per-pipeline alert frequency limiter with sliding window counting."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import time

from pipewatch.alerts import Alert, AlertLevel


@dataclass
class LimiterConfig:
    max_alerts_per_window: int = 5
    window_seconds: int = 300
    min_level: AlertLevel = AlertLevel.WARNING


@dataclass
class LimiterWindow:
    timestamps: List[float] = field(default_factory=list)

    def record(self, ts: Optional[float] = None) -> None:
        self.timestamps.append(ts if ts is not None else time.time())

    def prune(self, window_seconds: int, now: Optional[float] = None) -> None:
        cutoff = (now if now is not None else time.time()) - window_seconds
        self.timestamps = [t for t in self.timestamps if t >= cutoff]

    def count(self) -> int:
        return len(self.timestamps)


_state: Dict[str, LimiterWindow] = {}


def _key(alert: Alert) -> str:
    return f"{alert.pipeline}:{alert.level.value}"


def _get_window(alert: Alert) -> LimiterWindow:
    k = _key(alert)
    if k not in _state:
        _state[k] = LimiterWindow()
    return _state[k]


def record_alert(alert: Alert, now: Optional[float] = None) -> None:
    """Record that an alert was emitted."""
    _get_window(alert).record(now)


def should_limit(alert: Alert, cfg: LimiterConfig, now: Optional[float] = None) -> bool:
    """Return True if the alert should be suppressed due to rate limiting."""
    if alert.level < cfg.min_level:
        return False
    win = _get_window(alert)
    win.prune(cfg.window_seconds, now)
    return win.count() >= cfg.max_alerts_per_window


def apply_limiter(alerts: List[Alert], cfg: LimiterConfig, now: Optional[float] = None) -> List[Alert]:
    """Filter alerts that exceed the per-pipeline window limit."""
    allowed: List[Alert] = []
    for alert in alerts:
        if not should_limit(alert, cfg, now):
            allowed.append(alert)
            record_alert(alert, now)
    return allowed


def reset_limiter() -> None:
    """Clear all limiter state (useful for testing)."""
    _state.clear()
