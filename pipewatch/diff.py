"""Compute deltas between the current metric and a saved snapshot."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pipewatch.metrics import PipelineMetric, failure_rate, throughput


@dataclass
class MetricDiff:
    pipeline_name: str
    records_processed_delta: int
    records_failed_delta: int
    failure_rate_delta: float   # percentage points
    throughput_delta: float     # records/sec
    status_changed: bool
    previous_status: Optional[str]
    current_status: str


def compute_diff(current: PipelineMetric, previous: Optional[PipelineMetric]) -> Optional[MetricDiff]:
    """Return a MetricDiff if a previous snapshot exists, otherwise None."""
    if previous is None:
        return None

    current_fr = failure_rate(current)
    previous_fr = failure_rate(previous)
    current_tp = throughput(current)
    previous_tp = throughput(previous)

    return MetricDiff(
        pipeline_name=current.pipeline_name,
        records_processed_delta=current.records_processed - previous.records_processed,
        records_failed_delta=current.records_failed - previous.records_failed,
        failure_rate_delta=round(current_fr - previous_fr, 4),
        throughput_delta=round(current_tp - previous_tp, 4),
        status_changed=current.status != previous.status,
        previous_status=previous.status.value,
        current_status=current.status.value,
    )


def format_diff(diff: MetricDiff) -> str:
    """Render a human-readable summary of a MetricDiff."""
    lines = [
        f"[diff] {diff.pipeline_name}",
        f"  processed delta : {diff.records_processed_delta:+d}",
        f"  failed delta    : {diff.records_failed_delta:+d}",
        f"  failure rate Δ  : {diff.failure_rate_delta:+.2%}",
        f"  throughput Δ    : {diff.throughput_delta:+.2f} rec/s",
    ]
    if diff.status_changed:
        lines.append(f"  status change   : {diff.previous_status} -> {diff.current_status}")
    return "\n".join(lines)
