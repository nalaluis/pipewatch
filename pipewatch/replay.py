"""Replay historical pipeline snapshots for debugging and analysis."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.snapshot import load_snapshot, list_snapshots
from pipewatch.metrics import PipelineMetric
from pipewatch.health import evaluate_health, HealthThresholds
from pipewatch.alerts import build_alerts, filter_alerts, AlertLevel


@dataclass
class ReplayFrame:
    """A single frame in a replay sequence."""
    pipeline: str
    metric: PipelineMetric
    is_healthy: bool
    alert_count: int
    critical_count: int

    def __str__(self) -> str:
        status = "OK" if self.is_healthy else "DEGRADED"
        return (
            f"[{self.pipeline}] {status} | "
            f"failure_rate={self.metric.failure_rate:.2%} "
            f"throughput={self.metric.throughput:.1f} "
            f"alerts={self.alert_count} critical={self.critical_count}"
        )


def replay_pipeline(
    pipeline: str,
    thresholds: Optional[HealthThresholds] = None,
    min_level: AlertLevel = AlertLevel.WARNING,
) -> List[ReplayFrame]:
    """Replay all saved snapshots for a pipeline, returning a frame per snapshot."""
    if thresholds is None:
        thresholds = HealthThresholds()

    snapshots = list_snapshots()
    if pipeline not in snapshots:
        return []

    metric = load_snapshot(pipeline)
    if metric is None:
        return []

    result = evaluate_health(pipeline, metric, thresholds)
    alerts = filter_alerts(build_alerts(result), min_level=min_level)
    critical = [a for a in alerts if a.level == AlertLevel.CRITICAL]

    frame = ReplayFrame(
        pipeline=pipeline,
        metric=metric,
        is_healthy=result.is_healthy,
        alert_count=len(alerts),
        critical_count=len(critical),
    )
    return [frame]


def replay_all(
    thresholds: Optional[HealthThresholds] = None,
    min_level: AlertLevel = AlertLevel.WARNING,
) -> List[ReplayFrame]:
    """Replay snapshots for all known pipelines."""
    frames: List[ReplayFrame] = []
    for pipeline in list_snapshots():
        frames.extend(replay_pipeline(pipeline, thresholds=thresholds, min_level=min_level))
    return frames


def format_replay(frames: List[ReplayFrame]) -> str:
    """Render replay frames as a human-readable string."""
    if not frames:
        return "No replay data available."
    lines = ["=== Pipeline Replay ==="]
    for frame in frames:
        lines.append(str(frame))
    return "\n".join(lines)
