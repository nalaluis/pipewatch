"""Watchdog: detects stale pipelines that haven't reported metrics recently."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class WatchdogConfig:
    stale_after_seconds: int = 300  # 5 minutes
    enabled: bool = True


@dataclass
class StalePipeline:
    name: str
    last_seen: float
    seconds_since: float

    def __str__(self) -> str:
        return f"{self.name} (last seen {self.seconds_since:.0f}s ago)"


@dataclass
class WatchdogState:
    _last_seen: Dict[str, float] = field(default_factory=dict)

    def record(self, name: str, ts: Optional[float] = None) -> None:
        self._last_seen[name] = ts if ts is not None else time.time()

    def last_seen(self, name: str) -> Optional[float]:
        return self._last_seen.get(name)

    def all_names(self) -> List[str]:
        return list(self._last_seen.keys())

    def reset(self, name: str) -> None:
        self._last_seen.pop(name, None)


def check_stale(
    state: WatchdogState,
    config: WatchdogConfig,
    now: Optional[float] = None,
) -> List[StalePipeline]:
    """Return a list of pipelines that haven't been seen within the threshold."""
    if not config.enabled:
        return []
    now = now if now is not None else time.time()
    stale: List[StalePipeline] = []
    for name in state.all_names():
        last = state.last_seen(name)
        if last is None:
            continue
        elapsed = now - last
        if elapsed >= config.stale_after_seconds:
            stale.append(StalePipeline(name=name, last_seen=last, seconds_since=elapsed))
    return stale


def update_watchdog(state: WatchdogState, metric: PipelineMetric) -> None:
    """Record that a pipeline was just seen (called after collecting a metric)."""
    state.record(metric.pipeline_name)


def format_stale_report(stale: List[StalePipeline]) -> str:
    if not stale:
        return "All pipelines reporting on time."
    lines = ["STALE PIPELINES DETECTED:"]
    for sp in stale:
        lines.append(f"  - {sp}")
    return "\n".join(lines)
