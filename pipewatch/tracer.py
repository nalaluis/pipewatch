"""Pipeline execution tracer — records per-pipeline timing and status spans."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus


@dataclass
class TraceSpan:
    pipeline: str
    started_at: float
    ended_at: float
    status: PipelineStatus
    failure_rate: float
    throughput: float

    @property
    def duration_ms(self) -> float:
        return (self.ended_at - self.started_at) * 1000.0

    def __str__(self) -> str:
        return (
            f"[{self.pipeline}] {self.status.value} "
            f"dur={self.duration_ms:.1f}ms "
            f"failure_rate={self.failure_rate:.3f} "
            f"throughput={self.throughput:.1f}"
        )


@dataclass
class TraceReport:
    spans: List[TraceSpan] = field(default_factory=list)

    def add(self, span: TraceSpan) -> None:
        self.spans.append(span)

    def for_pipeline(self, name: str) -> List[TraceSpan]:
        return [s for s in self.spans if s.pipeline == name]

    def slowest(self, n: int = 5) -> List[TraceSpan]:
        return sorted(self.spans, key=lambda s: s.duration_ms, reverse=True)[:n]

    def pipeline_names(self) -> List[str]:
        seen: List[str] = []
        for s in self.spans:
            if s.pipeline not in seen:
                seen.append(s.pipeline)
        return seen


# Module-level registry keyed by pipeline name.
_active: Dict[str, float] = {}
_report: TraceReport = TraceReport()


def start_trace(pipeline: str) -> None:
    """Mark the start of a pipeline execution trace."""
    _active[pipeline] = time.monotonic()


def end_trace(metric: PipelineMetric) -> Optional[TraceSpan]:
    """Finish a trace for the pipeline described by *metric*.

    Returns the recorded :class:`TraceSpan` or ``None`` if
    :func:`start_trace` was never called for that pipeline.
    """
    started = _active.pop(metric.pipeline, None)
    if started is None:
        return None
    span = TraceSpan(
        pipeline=metric.pipeline,
        started_at=started,
        ended_at=time.monotonic(),
        status=metric.status,
        failure_rate=metric.failure_rate,
        throughput=metric.throughput,
    )
    _report.add(span)
    return span


def get_report() -> TraceReport:
    """Return the global :class:`TraceReport`."""
    return _report


def reset() -> None:
    """Clear all active traces and the accumulated report (useful in tests)."""
    _active.clear()
    _report.spans.clear()
