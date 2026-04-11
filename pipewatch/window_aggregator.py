"""Sliding window aggregator for pipeline metrics over time."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import time

from pipewatch.metrics import PipelineMetric


@dataclass
class WindowConfig:
    window_seconds: int = 300  # 5-minute default window
    max_entries: int = 100


@dataclass
class WindowEntry:
    metric: PipelineMetric
    recorded_at: float = field(default_factory=time.time)


@dataclass
class WindowStats:
    pipeline: str
    count: int
    avg_failure_rate: float
    avg_throughput: float
    min_failure_rate: float
    max_failure_rate: float
    min_throughput: float
    max_throughput: float
    window_seconds: int

    def __str__(self) -> str:
        return (
            f"[{self.pipeline}] window={self.window_seconds}s count={self.count} "
            f"avg_failure_rate={self.avg_failure_rate:.3f} "
            f"avg_throughput={self.avg_throughput:.1f}"
        )


_windows: Dict[str, List[WindowEntry]] = {}


def record_metric(metric: PipelineMetric, config: Optional[WindowConfig] = None) -> None:
    """Record a metric into the sliding window for its pipeline."""
    cfg = config or WindowConfig()
    name = metric.pipeline
    if name not in _windows:
        _windows[name] = []
    _windows[name].append(WindowEntry(metric=metric))
    _prune(name, cfg)


def _prune(pipeline: str, config: WindowConfig) -> None:
    """Remove entries outside the time window or beyond max_entries."""
    cutoff = time.time() - config.window_seconds
    entries = _windows.get(pipeline, [])
    entries = [e for e in entries if e.recorded_at >= cutoff]
    if len(entries) > config.max_entries:
        entries = entries[-config.max_entries:]
    _windows[pipeline] = entries


def compute_window_stats(
    pipeline: str,
    config: Optional[WindowConfig] = None,
) -> Optional[WindowStats]:
    """Compute aggregated stats over the current window for a pipeline."""
    cfg = config or WindowConfig()
    _prune(pipeline, cfg)
    entries = _windows.get(pipeline, [])
    if not entries:
        return None
    failure_rates = [e.metric.failure_rate for e in entries]
    throughputs = [e.metric.throughput for e in entries]
    return WindowStats(
        pipeline=pipeline,
        count=len(entries),
        avg_failure_rate=sum(failure_rates) / len(failure_rates),
        avg_throughput=sum(throughputs) / len(throughputs),
        min_failure_rate=min(failure_rates),
        max_failure_rate=max(failure_rates),
        min_throughput=min(throughputs),
        max_throughput=max(throughputs),
        window_seconds=cfg.window_seconds,
    )


def clear_window(pipeline: Optional[str] = None) -> None:
    """Clear window state for a pipeline, or all pipelines if None."""
    if pipeline is None:
        _windows.clear()
    else:
        _windows.pop(pipeline, None)
