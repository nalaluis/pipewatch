"""Automated remediation suggestions for pipeline health violations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.health import HealthResult


@dataclass
class RemediationAction:
    """A single suggested remediation action."""
    title: str
    description: str
    command: Optional[str] = None
    priority: int = 1  # lower = higher priority

    def __str__(self) -> str:
        parts = [f"[{self.priority}] {self.title}: {self.description}"]
        if self.command:
            parts.append(f"  $ {self.command}")
        return "\n".join(parts)


@dataclass
class RemediationReport:
    """Remediation suggestions for a pipeline."""
    pipeline: str
    actions: List[RemediationAction] = field(default_factory=list)

    @property
    def has_actions(self) -> bool:
        return len(self.actions) > 0

    def sorted_actions(self) -> List[RemediationAction]:
        return sorted(self.actions, key=lambda a: a.priority)


def _actions_for_alert(alert: Alert) -> List[RemediationAction]:
    """Return remediation actions based on alert violation type."""
    actions: List[RemediationAction] = []

    if "failure_rate" in alert.violation:
        actions.append(RemediationAction(
            title="Inspect recent failures",
            description="Review pipeline logs for error patterns causing elevated failure rate.",
            command=f"pipewatch logs --pipeline {alert.pipeline} --level error",
            priority=1,
        ))
        if alert.level == AlertLevel.CRITICAL:
            actions.append(RemediationAction(
                title="Consider circuit breaker",
                description="Enable circuit breaker to prevent cascading failures.",
                command=f"pipewatch breaker --pipeline {alert.pipeline} --enable",
                priority=2,
            ))

    if "throughput" in alert.violation:
        actions.append(RemediationAction(
            title="Check upstream data sources",
            description="Low throughput may indicate upstream feed delays or outages.",
            command=f"pipewatch check-upstream --pipeline {alert.pipeline}",
            priority=1,
        ))
        actions.append(RemediationAction(
            title="Scale worker capacity",
            description="Increase worker count if throughput drop is due to resource saturation.",
            priority=3,
        ))

    return actions


def build_remediation(pipeline: str, alerts: List[Alert]) -> RemediationReport:
    """Build a remediation report for a pipeline given its active alerts."""
    report = RemediationReport(pipeline=pipeline)
    seen: set = set()
    for alert in alerts:
        if alert.pipeline != pipeline:
            continue
        for action in _actions_for_alert(alert):
            key = (action.title, action.priority)
            if key not in seen:
                seen.add(key)
                report.actions.append(action)
    return report


def format_remediation(report: RemediationReport) -> str:
    """Render a remediation report as human-readable text."""
    if not report.has_actions:
        return f"[{report.pipeline}] No remediation actions suggested."
    lines = [f"[{report.pipeline}] Remediation suggestions:"]
    for action in report.sorted_actions():
        lines.append(str(action))
    return "\n".join(lines)
