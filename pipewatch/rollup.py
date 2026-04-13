"""Rollup: aggregate pipeline health results over a time window into summary buckets."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.health import HealthResult
from pipewatch.metrics import PipelineStatus


@dataclass
class RollupBucket:
    """A single time-window bucket of aggregated health results."""

    window_label: str
    total: int = 0
    healthy: int = 0
    warning: int = 0
    critical: int = 0
    avg_failure_rate: float = 0.0
    avg_throughput: float = 0.0

    def __str__(self) -> str:
        return (
            f"[{self.window_label}] total={self.total} "
            f"healthy={self.healthy} warning={self.warning} critical={self.critical} "
            f"avg_failure_rate={self.avg_failure_rate:.3f} avg_throughput={self.avg_throughput:.1f}"
        )


@dataclass
class RollupReport:
    """Collection of rollup buckets keyed by window label."""

    buckets: Dict[str, RollupBucket] = field(default_factory=dict)

    def labels(self) -> List[str]:
        return list(self.buckets.keys())

    def get(self, label: str) -> Optional[RollupBucket]:
        return self.buckets.get(label)


def rollup(
    results: List[HealthResult],
    window_label: str = "all",
) -> RollupBucket:
    """Aggregate a flat list of HealthResults into a single RollupBucket."""
    bucket = RollupBucket(window_label=window_label, total=len(results))
    if not results:
        return bucket

    total_fr = 0.0
    total_tp = 0.0
    for r in results:
        status = r.metric.status
        if status == PipelineStatus.HEALTHY:
            bucket.healthy += 1
        elif status == PipelineStatus.WARNING:
            bucket.warning += 1
        elif status == PipelineStatus.CRITICAL:
            bucket.critical += 1
        total_fr += r.metric.failure_rate
        total_tp += r.metric.throughput

    bucket.avg_failure_rate = total_fr / len(results)
    bucket.avg_throughput = total_tp / len(results)
    return bucket


def rollup_by_label(
    labeled_results: Dict[str, List[HealthResult]],
) -> RollupReport:
    """Build a RollupReport from a mapping of label -> list of HealthResults."""
    report = RollupReport()
    for label, results in labeled_results.items():
        report.buckets[label] = rollup(results, window_label=label)
    return report


def format_rollup(report: RollupReport) -> str:
    """Render a RollupReport as a human-readable string."""
    if not report.buckets:
        return "No rollup data available."
    lines = ["=== Rollup Report ==="]
    for label in sorted(report.labels()):
        lines.append(str(report.buckets[label]))
    return "\n".join(lines)
