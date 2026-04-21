"""Sentinel: tracks repeated critical alerts and escalates to a 'sentinel' state."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import time

from pipewatch.alerts import Alert, AlertLevel


@dataclass
class SentinelConfig:
    threshold: int = 3          # consecutive critical hits before sentinel fires
    window_seconds: float = 300 # rolling window to count hits in
    cooldown_seconds: float = 60 # min seconds between sentinel triggers


@dataclass
class SentinelEntry:
    pipeline: str
    hit_times: List[float] = field(default_factory=list)
    last_triggered: Optional[float] = None

    def prune(self, window_seconds: float, now: float) -> None:
        cutoff = now - window_seconds
        self.hit_times = [t for t in self.hit_times if t >= cutoff]

    def hit_count(self) -> int:
        return len(self.hit_times)


_state: Dict[str, SentinelEntry] = {}


def _get_entry(pipeline: str) -> SentinelEntry:
    if pipeline not in _state:
        _state[pipeline] = SentinelEntry(pipeline=pipeline)
    return _state[pipeline]


def reset(pipeline: Optional[str] = None) -> None:
    """Reset sentinel state for one or all pipelines."""
    if pipeline is None:
        _state.clear()
    else:
        _state.pop(pipeline, None)


def record_hit(pipeline: str, config: SentinelConfig, now: Optional[float] = None) -> None:
    """Record a critical alert hit for a pipeline."""
    now = now or time.time()
    entry = _get_entry(pipeline)
    entry.hit_times.append(now)
    entry.prune(config.window_seconds, now)


def should_trigger(pipeline: str, config: SentinelConfig, now: Optional[float] = None) -> bool:
    """Return True if the sentinel threshold has been crossed and cooldown has elapsed."""
    now = now or time.time()
    entry = _get_entry(pipeline)
    entry.prune(config.window_seconds, now)
    if entry.hit_count() < config.threshold:
        return False
    if entry.last_triggered is not None:
        if (now - entry.last_triggered) < config.cooldown_seconds:
            return False
    return True


def mark_triggered(pipeline: str, now: Optional[float] = None) -> None:
    """Record that the sentinel was triggered for a pipeline."""
    now = now or time.time()
    entry = _get_entry(pipeline)
    entry.last_triggered = now


def apply_sentinel(alerts: List[Alert], config: SentinelConfig, now: Optional[float] = None) -> List[Alert]:
    """Process alerts, record hits, and append sentinel alerts where triggered."""
    now = now or time.time()
    sentinel_alerts: List[Alert] = []
    for alert in alerts:
        if alert.level == AlertLevel.CRITICAL:
            record_hit(alert.pipeline, config, now)
            if should_trigger(alert.pipeline, config, now):
                mark_triggered(alert.pipeline, now)
                sentinel_alerts.append(
                    Alert(
                        pipeline=alert.pipeline,
                        level=AlertLevel.CRITICAL,
                        metric=alert.metric,
                        message=f"[SENTINEL] {alert.pipeline} has triggered {config.threshold}+ critical alerts within {config.window_seconds}s",
                    )
                )
    return sentinel_alerts
