"""Velocity tracking: measures how fast pipeline metrics are changing over time."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class VelocityConfig:
    min_samples: int = 3
    window_size: int = 10


@dataclass
class VelocityResult:
    pipeline: str
    failure_rate_velocity: float  # change per sample (positive = worsening)
    throughput_velocity: float    # change per sample (positive = improving)
    samples_used: int

    def __str__(self) -> str:
        fr_dir = "+" if self.failure_rate_velocity >= 0 else ""
        tp_dir = "+" if self.throughput_velocity >= 0 else ""
        return (
            f"{self.pipeline}: "
            f"failure_rate_velocity={fr_dir}{self.failure_rate_velocity:.4f}/sample "
            f"throughput_velocity={tp_dir}{self.throughput_velocity:.4f}/sample "
            f"(n={self.samples_used})"
        )


_history: Dict[str, List[PipelineMetric]] = {}


def _get_history(pipeline: str) -> List[PipelineMetric]:
    return _history.setdefault(pipeline, [])


def record_metric(metric: PipelineMetric, cfg: Optional[VelocityConfig] = None) -> None:
    """Append a metric sample for the given pipeline."""
    cfg = cfg or VelocityConfig()
    buf = _get_history(metric.pipeline)
    buf.append(metric)
    if len(buf) > cfg.window_size:
        buf.pop(0)


def compute_velocity(
    pipeline: str,
    cfg: Optional[VelocityConfig] = None,
) -> Optional[VelocityResult]:
    """Compute per-sample velocity for failure_rate and throughput.

    Returns None if there are not enough samples.
    """
    cfg = cfg or VelocityConfig()
    buf = _get_history(pipeline)
    n = len(buf)
    if n < cfg.min_samples:
        return None

    fr_vals = [m.failure_rate for m in buf]
    tp_vals = [m.throughput for m in buf]

    def _slope(vals: List[float]) -> float:
        if len(vals) < 2:
            return 0.0
        xs = list(range(len(vals)))
        x_mean = sum(xs) / len(xs)
        y_mean = sum(vals) / len(vals)
        num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, vals))
        den = sum((x - x_mean) ** 2 for x in xs)
        return num / den if den != 0.0 else 0.0

    return VelocityResult(
        pipeline=pipeline,
        failure_rate_velocity=_slope(fr_vals),
        throughput_velocity=_slope(tp_vals),
        samples_used=n,
    )


def reset_velocity(pipeline: Optional[str] = None) -> None:
    """Clear velocity history for one pipeline or all."""
    if pipeline is None:
        _history.clear()
    else:
        _history.pop(pipeline, None)
