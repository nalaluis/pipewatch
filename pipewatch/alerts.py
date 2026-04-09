"""Alert formatting and notification for pipeline health events."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Optional

from pipewatch.health import HealthResult
from pipewatch.metrics import PipelineStatus


class AlertLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class Alert:
    pipeline_name: str
    level: AlertLevel
    message: str
    timestamp: datetime
    metric_name: Optional[str] = None
    metric_value: Optional[float] = None

    def format(self) -> str:
        ts = self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        parts = [f"[{ts}]", f"[{self.level.upper()}]", f"{self.pipeline_name}:", self.message]
        if self.metric_name and self.metric_value is not None:
            parts.append(f"({self.metric_name}={self.metric_value:.4f})")
        return " ".join(parts)


def build_alerts(pipeline_name: str, result: HealthResult) -> List[Alert]:
    """Convert a HealthResult into a list of Alert objects."""
    alerts: List[Alert] = []
    now = datetime.utcnow()

    if result.status == PipelineStatus.HEALTHY:
        return alerts

    for violation in result.violations:
        level = (
            AlertLevel.CRITICAL
            if result.status == PipelineStatus.CRITICAL
            else AlertLevel.WARNING
        )
        alerts.append(
            Alert(
                pipeline_name=pipeline_name,
                level=level,
                message=violation,
                timestamp=now,
            )
        )

    return alerts


def filter_alerts(alerts: List[Alert], min_level: AlertLevel) -> List[Alert]:
    """Return only alerts at or above the given minimum severity level.

    Severity order (ascending): INFO < WARNING < CRITICAL.
    """
    order = [AlertLevel.INFO, AlertLevel.WARNING, AlertLevel.CRITICAL]
    threshold = order.index(min_level)
    return [a for a in alerts if order.index(a.level) >= threshold]


def emit_alerts(alerts: List[Alert], sink=None) -> None:
    """Emit alerts to a sink (defaults to stdout)."""
    import sys

    out = sink or sys.stdout
    for alert in alerts:
        out.write(alert.format() + "\n")
