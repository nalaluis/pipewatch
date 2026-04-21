"""Pipeline inspector: deep-dive diagnostic summary for a single pipeline."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric
from pipewatch.health import HealthResult
from pipewatch.alerts import Alert, AlertLevel
from pipewatch.scorer import PipelineScore
from pipewatch.trend import TrendSummary
from pipewatch.anomaly import AnomalyResult


@dataclass
class InspectionReport:
    pipeline: str
    metric: PipelineMetric
    health: HealthResult
    alerts: List[Alert]
    score: Optional[PipelineScore] = None
    trend: Optional[TrendSummary] = None
    anomalies: List[AnomalyResult] = field(default_factory=list)

    @property
    def has_anomalies(self) -> bool:
        return len(self.anomalies) > 0

    @property
    def critical_alerts(self) -> List[Alert]:
        return [a for a in self.alerts if a.level == AlertLevel.CRITICAL]

    @property
    def warning_alerts(self) -> List[Alert]:
        return [a for a in self.alerts if a.level == AlertLevel.WARNING]


def inspect_pipeline(
    pipeline: str,
    metric: PipelineMetric,
    health: HealthResult,
    alerts: List[Alert],
    score: Optional[PipelineScore] = None,
    trend: Optional[TrendSummary] = None,
    anomalies: Optional[List[AnomalyResult]] = None,
) -> InspectionReport:
    """Assemble a full diagnostic report for a single pipeline."""
    return InspectionReport(
        pipeline=pipeline,
        metric=metric,
        health=health,
        alerts=alerts,
        score=score,
        trend=trend,
        anomalies=anomalies or [],
    )


def format_inspection(report: InspectionReport) -> str:
    """Render an InspectionReport as a human-readable string."""
    lines: List[str] = [
        f"=== Pipeline Inspection: {report.pipeline} ===",
        f"  Status       : {report.health.status.value}",
        f"  Failure Rate : {report.metric.failure_rate:.2%}",
        f"  Throughput   : {report.metric.throughput:.1f} rec/s",
    ]

    if report.score is not None:
        lines.append(f"  Score        : {report.score.score:.1f} / 100  (grade {report.score.grade})")

    if report.trend is not None:
        lines.append(f"  Trend        : {report.trend.failure_rate_label} failure rate, "
                     f"{report.trend.throughput_label} throughput")

    if report.alerts:
        lines.append(f"  Alerts ({len(report.alerts)}):")
        for a in report.alerts:
            lines.append(f"    [{a.level.value.upper()}] {a.message}")
    else:
        lines.append("  Alerts       : none")

    if report.has_anomalies:
        lines.append(f"  Anomalies ({len(report.anomalies)}):")
        for an in report.anomalies:
            lines.append(f"    {an}")

    return "\n".join(lines)
