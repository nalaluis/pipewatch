"""Anomaly detection for pipeline metrics using simple statistical thresholds."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.metrics import PipelineMetric
from pipewatch.history import HistoryEntry


@dataclass
class AnomalyConfig:
    z_score_threshold: float = 2.5
    min_history: int = 3


@dataclass
class AnomalyResult:
    pipeline: str
    metric_name: str
    current_value: float
    mean: float
    std_dev: float
    z_score: float
    is_anomaly: bool

    def __str__(self) -> str:
        flag = "ANOMALY" if self.is_anomaly else "ok"
        return (
            f"[{flag}] {self.pipeline}/{self.metric_name}: "
            f"value={self.current_value:.4f} mean={self.mean:.4f} "
            f"std={self.std_dev:.4f} z={self.z_score:.2f}"
        )


def _mean(values: List[float]) -> float:
    return sum(values) / len(values)


def _std_dev(values: List[float], mean: float) -> float:
    if len(values) < 2:
        return 0.0
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return variance ** 0.5


def detect_anomaly(
    current: PipelineMetric,
    history: List[HistoryEntry],
    config: Optional[AnomalyConfig] = None,
) -> List[AnomalyResult]:
    """Detect anomalies in failure_rate and throughput against historical entries."""
    cfg = config or AnomalyConfig()
    results: List[AnomalyResult] = []

    if len(history) < cfg.min_history:
        return results

    checks = [
        ("failure_rate", current.failure_rate, [e.metric.failure_rate for e in history]),
        ("throughput", current.throughput, [e.metric.throughput for e in history]),
    ]

    for metric_name, current_value, hist_values in checks:
        mean = _mean(hist_values)
        std = _std_dev(hist_values, mean)
        if std == 0.0:
            z = 0.0
        else:
            z = abs(current_value - mean) / std
        is_anomaly = z >= cfg.z_score_threshold
        results.append(
            AnomalyResult(
                pipeline=current.pipeline,
                metric_name=metric_name,
                current_value=current_value,
                mean=mean,
                std_dev=std,
                z_score=z,
                is_anomaly=is_anomaly,
            )
        )

    return results


def format_anomalies(results: List[AnomalyResult]) -> str:
    if not results:
        return "No anomalies detected."
    lines = [str(r) for r in results]
    return "\n".join(lines)
