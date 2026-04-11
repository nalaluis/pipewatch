"""Alert budget tracking: limits total alerts fired within a rolling window."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List

from pipewatch.alerts import Alert, AlertLevel


@dataclass
class BudgetConfig:
    max_alerts: int = 50
    window_seconds: int = 3600
    per_pipeline: bool = False


@dataclass
class BudgetWindow:
    _timestamps: List[float] = field(default_factory=list)

    def record(self, ts: float) -> None:
        self._timestamps.append(ts)

    def prune(self, cutoff: float) -> None:
        self._timestamps = [t for t in self._timestamps if t >= cutoff]

    def count(self) -> int:
        return len(self._timestamps)


_global_window: BudgetWindow = BudgetWindow()
_pipeline_windows: Dict[str, BudgetWindow] = {}


def _now() -> float:
    return datetime.now(timezone.utc).timestamp()


def _window_for(pipeline: str, per_pipeline: bool) -> BudgetWindow:
    global _global_window
    if not per_pipeline:
        return _global_window
    if pipeline not in _pipeline_windows:
        _pipeline_windows[pipeline] = BudgetWindow()
    return _pipeline_windows[pipeline]


def reset_budget() -> None:
    global _global_window
    _global_window = BudgetWindow()
    _pipeline_windows.clear()


def budget_exhausted(alert: Alert, config: BudgetConfig) -> bool:
    """Return True if the alert budget has been exceeded for this alert's scope."""
    now = _now()
    cutoff = now - config.window_seconds
    window = _window_for(alert.pipeline, config.per_pipeline)
    window.prune(cutoff)
    return window.count() >= config.max_alerts


def record_alert(alert: Alert, config: BudgetConfig) -> None:
    """Record that an alert was fired."""
    window = _window_for(alert.pipeline, config.per_pipeline)
    window.record(_now())


def apply_budget(alerts: List[Alert], config: BudgetConfig) -> List[Alert]:
    """Filter alerts that exceed the budget; record those that pass."""
    allowed: List[Alert] = []
    for alert in alerts:
        if not budget_exhausted(alert, config):
            record_alert(alert, config)
            allowed.append(alert)
    return allowed
