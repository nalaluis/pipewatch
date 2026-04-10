"""Correlate alerts across pipelines to detect related failures."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.alerts import Alert, AlertLevel


@dataclass
class CorrelationGroup:
    """A group of alerts that share a common correlation key."""

    key: str
    alerts: List[Alert] = field(default_factory=list)

    @property
    def size(self) -> int:
        return len(self.alerts)

    @property
    def max_level(self) -> Optional[AlertLevel]:
        if not self.alerts:
            return None
        order = {AlertLevel.WARNING: 0, AlertLevel.CRITICAL: 1}
        return max(self.alerts, key=lambda a: order.get(a.level, -1)).level

    @property
    def pipeline_names(self) -> List[str]:
        return [a.pipeline for a in self.alerts]


def correlate_by_level(alerts: List[Alert]) -> Dict[str, CorrelationGroup]:
    """Group alerts by their alert level."""
    groups: Dict[str, CorrelationGroup] = {}
    for alert in alerts:
        key = alert.level.value
        if key not in groups:
            groups[key] = CorrelationGroup(key=key)
        groups[key].alerts.append(alert)
    return groups


def correlate_by_metric(alerts: List[Alert]) -> Dict[str, CorrelationGroup]:
    """Group alerts by the metric type mentioned in the message."""
    groups: Dict[str, CorrelationGroup] = {}
    for alert in alerts:
        key = "throughput" if "throughput" in alert.message.lower() else "failure_rate"
        if key not in groups:
            groups[key] = CorrelationGroup(key=key)
        groups[key].alerts.append(alert)
    return groups


def format_correlation(groups: Dict[str, CorrelationGroup]) -> str:
    """Render correlation groups as a human-readable string."""
    if not groups:
        return "No correlated alerts."
    lines = []
    for key, group in groups.items():
        lines.append(f"[{key.upper()}] {group.size} alert(s): {', '.join(group.pipeline_names)}")
    return "\n".join(lines)
