"""Pipeline alert quota enforcement — limits total alerts per pipeline per time window."""

from __future__ import annotations

from dataclasses import dataclass, field
from time import time
from typing import Dict, List, Optional

from pipewatch.alerts import Alert


@dataclass
class QuotaConfig:
    max_alerts: int = 10
    window_seconds: int = 3600


@dataclass
class QuotaWindow:
    timestamps: List[float] = field(default_factory=list)

    def record(self, ts: Optional[float] = None) -> None:
        self.timestamps.append(ts if ts is not None else time())

    def prune(self, window_seconds: int, now: Optional[float] = None) -> None:
        cutoff = (now if now is not None else time()) - window_seconds
        self.timestamps = [t for t in self.timestamps if t >= cutoff]

    def count(self) -> int:
        return len(self.timestamps)


@dataclass
class QuotaState:
    _windows: Dict[str, QuotaWindow] = field(default_factory=dict)

    def _get(self, pipeline: str) -> QuotaWindow:
        if pipeline not in self._windows:
            self._windows[pipeline] = QuotaWindow()
        return self._windows[pipeline]

    def record(self, pipeline: str, ts: Optional[float] = None) -> None:
        self._get(pipeline).record(ts)

    def count(self, pipeline: str, window_seconds: int, now: Optional[float] = None) -> int:
        w = self._get(pipeline)
        w.prune(window_seconds, now)
        return w.count()

    def reset(self, pipeline: str) -> None:
        self._windows.pop(pipeline, None)


_state = QuotaState()


def is_quota_exceeded(pipeline: str, cfg: QuotaConfig, now: Optional[float] = None) -> bool:
    return _state.count(pipeline, cfg.window_seconds, now) >= cfg.max_alerts


def record_alert(pipeline: str, ts: Optional[float] = None) -> None:
    _state.record(pipeline, ts)


def apply_quota(alerts: List[Alert], cfg: QuotaConfig, now: Optional[float] = None) -> List[Alert]:
    """Return only alerts that have not exceeded their pipeline quota."""
    allowed: List[Alert] = []
    for alert in alerts:
        name = alert.pipeline
        if not is_quota_exceeded(name, cfg, now):
            record_alert(name, now)
            allowed.append(alert)
    return allowed


def get_state() -> QuotaState:
    return _state


def reset_state() -> None:
    global _state
    _state = QuotaState()
