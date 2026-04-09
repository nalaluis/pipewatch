"""Aggregates metrics across multiple pipelines into a summary view."""

from dataclasses import dataclass, field
from typing import List, Dict

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.health import HealthResult


@dataclass
class PipelineSummary:
    total: int = 0
    healthy: int = 0
    warning: int = 0
    critical: int = 0
    avg_failure_rate: float = 0.0
    avg_throughput: float = 0.0
    worst_pipeline: str = ""
    worst_failure_rate: float = 0.0


def aggregate(results: List[HealthResult]) -> PipelineSummary:
    """Compute an aggregated summary from a list of HealthResult objects."""
    if not results:
        return PipelineSummary()

    total = len(results)
    healthy = sum(1 for r in results if r.status == PipelineStatus.HEALTHY)
    warning = sum(1 for r in results if r.status == PipelineStatus.WARNING)
    critical = sum(1 for r in results if r.status == PipelineStatus.CRITICAL)

    avg_failure_rate = sum(r.metric.failure_rate for r in results) / total
    avg_throughput = sum(r.metric.throughput for r in results) / total

    worst = max(results, key=lambda r: r.metric.failure_rate)
    worst_pipeline = worst.metric.pipeline_name
    worst_failure_rate = worst.metric.failure_rate

    return PipelineSummary(
        total=total,
        healthy=healthy,
        warning=warning,
        critical=critical,
        avg_failure_rate=round(avg_failure_rate, 4),
        avg_throughput=round(avg_throughput, 2),
        worst_pipeline=worst_pipeline,
        worst_failure_rate=round(worst_failure_rate, 4),
    )


def format_summary(summary: PipelineSummary) -> str:
    """Render a PipelineSummary as a human-readable string."""
    lines = [
        "=== Pipeline Aggregate Summary ===",
        f"  Total pipelines : {summary.total}",
        f"  Healthy         : {summary.healthy}",
        f"  Warning         : {summary.warning}",
        f"  Critical        : {summary.critical}",
        f"  Avg failure rate: {summary.avg_failure_rate:.2%}",
        f"  Avg throughput  : {summary.avg_throughput:.1f} rec/s",
    ]
    if summary.worst_pipeline:
        lines.append(
            f"  Worst pipeline  : {summary.worst_pipeline} "
            f"(failure rate {summary.worst_failure_rate:.2%})"
        )
    return "\n".join(lines)
