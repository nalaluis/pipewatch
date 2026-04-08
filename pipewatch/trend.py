"""Trend analysis over pipeline history entries."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import HistoryEntry


@dataclass
class TrendSummary:
    pipeline: str
    sample_count: int
    avg_failure_rate: float
    avg_throughput: float
    failure_rate_trend: str   # 'improving', 'degrading', 'stable'
    throughput_trend: str     # 'improving', 'degrading', 'stable'
    consecutive_critical: int


_STABLE_THRESHOLD = 0.05  # 5 % relative change considered stable


def _relative_change(first: float, last: float) -> float:
    """Return relative change from *first* to *last*."""
    if first == 0.0:
        return 0.0
    return (last - first) / abs(first)


def _trend_label(values: List[float], higher_is_better: bool) -> str:
    """Classify a list of scalar values as improving / degrading / stable."""
    if len(values) < 2:
        return "stable"
    change = _relative_change(values[0], values[-1])
    if abs(change) < _STABLE_THRESHOLD:
        return "stable"
    improved = change < 0 if higher_is_better is False else change > 0
    return "improving" if improved else "degrading"


def compute_trend(entries: List[HistoryEntry]) -> Optional[TrendSummary]:
    """Compute a :class:`TrendSummary` from an ordered list of history entries.

    Returns *None* when *entries* is empty.
    """
    if not entries:
        return None

    pipeline = entries[0].current.pipeline
    failure_rates = [e.current.failure_rate for e in entries]
    throughputs = [e.current.throughput for e in entries]

    avg_fr = sum(failure_rates) / len(failure_rates)
    avg_tp = sum(throughputs) / len(throughputs)

    from pipewatch.metrics import PipelineStatus
    consecutive_critical = 0
    for entry in reversed(entries):
        if entry.current.status == PipelineStatus.CRITICAL:
            consecutive_critical += 1
        else:
            break

    return TrendSummary(
        pipeline=pipeline,
        sample_count=len(entries),
        avg_failure_rate=round(avg_fr, 4),
        avg_throughput=round(avg_tp, 4),
        failure_rate_trend=_trend_label(failure_rates, higher_is_better=False),
        throughput_trend=_trend_label(throughputs, higher_is_better=True),
        consecutive_critical=consecutive_critical,
    )


def format_trend(summary: TrendSummary) -> str:
    """Return a human-readable single-line summary of trend data."""
    return (
        f"[{summary.pipeline}] samples={summary.sample_count} "
        f"avg_failure_rate={summary.avg_failure_rate:.2%} ({summary.failure_rate_trend}) "
        f"avg_throughput={summary.avg_throughput:.1f}/s ({summary.throughput_trend}) "
        f"consecutive_critical={summary.consecutive_critical}"
    )
