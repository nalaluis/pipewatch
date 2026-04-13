"""Pipeline metric comparator: compare two reports and surface regressions or improvements."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.reporter import Report


@dataclass
class PipelineComparison:
    pipeline: str
    prev_status: PipelineStatus
    curr_status: PipelineStatus
    prev_failure_rate: float
    curr_failure_rate: float
    prev_throughput: float
    curr_throughput: float

    @property
    def failure_rate_delta(self) -> float:
        return self.curr_failure_rate - self.prev_failure_rate

    @property
    def throughput_delta(self) -> float:
        return self.curr_throughput - self.prev_throughput

    @property
    def is_regression(self) -> bool:
        status_order = {
            PipelineStatus.HEALTHY: 0,
            PipelineStatus.WARNING: 1,
            PipelineStatus.CRITICAL: 2,
        }
        return status_order[self.curr_status] > status_order[self.prev_status]

    @property
    def is_improvement(self) -> bool:
        status_order = {
            PipelineStatus.HEALTHY: 0,
            PipelineStatus.WARNING: 1,
            PipelineStatus.CRITICAL: 2,
        }
        return status_order[self.curr_status] < status_order[self.prev_status]

    def __str__(self) -> str:
        direction = "↑" if self.is_regression else ("↓" if self.is_improvement else "→")
        return (
            f"{self.pipeline}: {self.prev_status.value} {direction} {self.curr_status.value} "
            f"| failure_rate Δ{self.failure_rate_delta:+.2%} "
            f"| throughput Δ{self.throughput_delta:+.1f}/s"
        )


@dataclass
class ComparisonReport:
    comparisons: List[PipelineComparison]

    @property
    def regressions(self) -> List[PipelineComparison]:
        return [c for c in self.comparisons if c.is_regression]

    @property
    def improvements(self) -> List[PipelineComparison]:
        return [c for c in self.comparisons if c.is_improvement]


def compare_reports(prev: Report, curr: Report) -> ComparisonReport:
    """Compare two reports and return a ComparisonReport with per-pipeline diffs."""
    prev_map = {r.metric.pipeline: r for r in prev.results}
    curr_map = {r.metric.pipeline: r for r in curr.results}

    comparisons: List[PipelineComparison] = []
    for name, curr_result in curr_map.items():
        if name not in prev_map:
            continue
        prev_result = prev_map[name]
        comparisons.append(
            PipelineComparison(
                pipeline=name,
                prev_status=prev_result.status,
                curr_status=curr_result.status,
                prev_failure_rate=prev_result.metric.failure_rate,
                curr_failure_rate=curr_result.metric.failure_rate,
                prev_throughput=prev_result.metric.throughput,
                curr_throughput=curr_result.metric.throughput,
            )
        )
    return ComparisonReport(comparisons=comparisons)


def format_comparison(report: ComparisonReport) -> str:
    """Render a human-readable comparison report."""
    if not report.comparisons:
        return "No comparable pipelines found."
    lines = ["=== Pipeline Comparison ==="]
    for c in report.comparisons:
        lines.append(f"  {c}")
    lines.append(f"Regressions: {len(report.regressions)}  Improvements: {len(report.improvements)}")
    return "\n".join(lines)
