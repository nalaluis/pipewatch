"""High-level helpers that integrate baseline checks into the watch loop."""
from __future__ import annotations

from typing import List, Optional

from pipewatch.baseline import (
    BaselineComparison,
    compare_to_baseline,
    format_comparison,
    save_baseline,
)
from pipewatch.metrics import PipelineMetric
from pipewatch.reporter import Report


def run_baseline_check(
    report: Report,
    regression_threshold: float = 0.05,
) -> List[BaselineComparison]:
    """Return baseline comparisons for every pipeline in *report*.

    Pipelines without a stored baseline are silently skipped.
    """
    comparisons: List[BaselineComparison] = []
    for result in report.results:
        cmp = compare_to_baseline(
            result.metric, regression_threshold=regression_threshold
        )
        if cmp is not None:
            comparisons.append(cmp)
    return comparisons


def capture_baselines(report: Report) -> List[str]:
    """Persist current metrics as baselines for all pipelines in *report*.

    Returns a list of pipeline names that were saved.
    """
    saved: List[str] = []
    for result in report.results:
        save_baseline(result.metric)
        saved.append(result.metric.pipeline)
    return saved


def regressions_in_report(
    report: Report,
    regression_threshold: float = 0.05,
) -> List[BaselineComparison]:
    """Convenience wrapper — returns only comparisons flagged as regressions."""
    return [
        c
        for c in run_baseline_check(report, regression_threshold)
        if c.regression
    ]


def format_baseline_report(comparisons: List[BaselineComparison]) -> str:
    """Render all comparisons as a multi-line string."""
    if not comparisons:
        return "No baseline comparisons available."
    lines = ["Baseline Comparison:"] + [f"  {format_comparison(c)}" for c in comparisons]
    return "\n".join(lines)
