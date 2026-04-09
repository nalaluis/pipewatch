"""Baseline management: capture, compare, and report metric baselines."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional
import json
import os

from pipewatch.metrics import PipelineMetric, failure_rate, throughput

_BASELINE_DIR = os.path.join(".pipewatch", "baselines")


@dataclass
class BaselineEntry:
    pipeline: str
    failure_rate: float
    throughput: float
    recorded_at: str


@dataclass
class BaselineComparison:
    pipeline: str
    failure_rate_delta: float          # current - baseline
    throughput_delta: float            # current - baseline
    regression: bool = False           # True when failure_rate_delta > threshold


def _baseline_path(pipeline: str) -> str:
    os.makedirs(_BASELINE_DIR, exist_ok=True)
    safe = pipeline.replace("/", "_").replace(" ", "_")
    return os.path.join(_BASELINE_DIR, f"{safe}.json")


def save_baseline(metric: PipelineMetric) -> BaselineEntry:
    """Persist current metric values as the baseline for a pipeline."""
    from pipewatch.reporter import _timestamp

    entry = BaselineEntry(
        pipeline=metric.pipeline,
        failure_rate=failure_rate(metric),
        throughput=throughput(metric),
        recorded_at=_timestamp(),
    )
    path = _baseline_path(metric.pipeline)
    with open(path, "w") as fh:
        json.dump(entry.__dict__, fh, indent=2)
    return entry


def load_baseline(pipeline: str) -> Optional[BaselineEntry]:
    """Return the stored baseline for *pipeline*, or None if absent."""
    path = _baseline_path(pipeline)
    if not os.path.exists(path):
        return None
    with open(path) as fh:
        data = json.load(fh)
    return BaselineEntry(**data)


def compare_to_baseline(
    metric: PipelineMetric,
    regression_threshold: float = 0.05,
) -> Optional[BaselineComparison]:
    """Compare *metric* against its stored baseline.

    Returns None when no baseline exists yet.
    *regression_threshold*: failure-rate increase that triggers regression flag.
    """
    baseline = load_baseline(metric.pipeline)
    if baseline is None:
        return None

    fr_delta = failure_rate(metric) - baseline.failure_rate
    tp_delta = throughput(metric) - baseline.throughput
    return BaselineComparison(
        pipeline=metric.pipeline,
        failure_rate_delta=round(fr_delta, 6),
        throughput_delta=round(tp_delta, 4),
        regression=fr_delta > regression_threshold,
    )


def format_comparison(cmp: BaselineComparison) -> str:
    """Return a human-readable one-liner for a BaselineComparison."""
    reg_tag = " [REGRESSION]" if cmp.regression else ""
    return (
        f"{cmp.pipeline}: failure_rate Δ{cmp.failure_rate_delta:+.2%}  "
        f"throughput Δ{cmp.throughput_delta:+.2f}/s{reg_tag}"
    )
