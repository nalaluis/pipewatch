"""Health evaluation logic: compares metrics against thresholds."""

from dataclasses import dataclass
from typing import List

from pipewatch.metrics import PipelineMetric, PipelineStatus


@dataclass
class HealthThresholds:
    max_failure_rate: float = 0.05       # 5%
    min_throughput: float = 10.0         # rows/sec
    max_duration_seconds: float = 3600.0 # 1 hour
    warning_failure_rate: float = 0.01   # 1%
    warning_throughput: float = 50.0     # rows/sec


@dataclass
class HealthResult:
    pipeline_id: str
    status: PipelineStatus
    reasons: List[str]

    def is_healthy(self) -> bool:
        return self.status == PipelineStatus.OK


def evaluate_health(
    metric: PipelineMetric,
    thresholds: HealthThresholds | None = None,
) -> HealthResult:
    """Evaluate a PipelineMetric against thresholds and return a HealthResult."""
    if thresholds is None:
        thresholds = HealthThresholds()

    reasons: List[str] = []
    status = PipelineStatus.OK

    if metric.failure_rate >= thresholds.max_failure_rate:
        status = PipelineStatus.CRITICAL
        reasons.append(
            f"Failure rate {metric.failure_rate:.2%} exceeds critical threshold "
            f"{thresholds.max_failure_rate:.2%}"
        )
    elif metric.failure_rate >= thresholds.warning_failure_rate:
        if status == PipelineStatus.OK:
            status = PipelineStatus.WARNING
        reasons.append(
            f"Failure rate {metric.failure_rate:.2%} exceeds warning threshold "
            f"{thresholds.warning_failure_rate:.2%}"
        )

    if metric.throughput < thresholds.min_throughput:
        status = PipelineStatus.CRITICAL
        reasons.append(
            f"Throughput {metric.throughput:.1f} rows/s below critical threshold "
            f"{thresholds.min_throughput:.1f} rows/s"
        )
    elif metric.throughput < thresholds.warning_throughput:
        if status == PipelineStatus.OK:
            status = PipelineStatus.WARNING
        reasons.append(
            f"Throughput {metric.throughput:.1f} rows/s below warning threshold "
            f"{thresholds.warning_throughput:.1f} rows/s"
        )

    if metric.duration_seconds > thresholds.max_duration_seconds:
        status = PipelineStatus.CRITICAL
        reasons.append(
            f"Duration {metric.duration_seconds:.0f}s exceeds max "
            f"{thresholds.max_duration_seconds:.0f}s"
        )

    return HealthResult(pipeline_id=metric.pipeline_id, status=status, reasons=reasons)
