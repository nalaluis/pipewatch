"""Runbook suggestions: map pipeline alert conditions to remediation steps."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.alerts import Alert, AlertLevel


@dataclass
class RunbookEntry:
    title: str
    steps: List[str]
    reference: Optional[str] = None

    def format(self) -> str:
        lines = [f"  Runbook: {self.title}"]
        for i, step in enumerate(self.steps, 1):
            lines.append(f"    {i}. {step}")
        if self.reference:
            lines.append(f"    Ref: {self.reference}")
        return "\n".join(lines)


@dataclass
class RunbookSuggestion:
    alert: Alert
    entry: RunbookEntry

    def format(self) -> str:
        return f"[{self.alert.level.value.upper()}] {self.alert.pipeline}\n{self.entry.format()}"


_DEFAULT_RUNBOOKS: dict = {
    "failure_rate": RunbookEntry(
        title="High Failure Rate",
        steps=[
            "Check pipeline logs for recurring errors.",
            "Verify upstream data source availability.",
            "Review recent deployments or schema changes.",
            "Restart the pipeline if transient errors are suspected.",
        ],
        reference="https://wiki.example.com/runbooks/failure-rate",
    ),
    "throughput": RunbookEntry(
        title="Low Throughput",
        steps=[
            "Inspect resource utilisation (CPU, memory, I/O).",
            "Check for back-pressure from downstream consumers.",
            "Review batch sizes and parallelism settings.",
            "Escalate to on-call if throughput remains below threshold.",
        ],
        reference="https://wiki.example.com/runbooks/low-throughput",
    ),
}


def _runbook_for_alert(alert: Alert) -> Optional[RunbookEntry]:
    """Return the best-matching runbook entry for an alert."""
    reason_lower = alert.reason.lower()
    for key, entry in _DEFAULT_RUNBOOKS.items():
        if key in reason_lower:
            return entry
    return None


def suggest_runbooks(alerts: List[Alert]) -> List[RunbookSuggestion]:
    """Return runbook suggestions for each actionable alert."""
    suggestions: List[RunbookSuggestion] = []
    for alert in alerts:
        if alert.level == AlertLevel.OK:
            continue
        entry = _runbook_for_alert(alert)
        if entry:
            suggestions.append(RunbookSuggestion(alert=alert, entry=entry))
    return suggestions


def format_runbook_report(suggestions: List[RunbookSuggestion]) -> str:
    """Render all runbook suggestions as a human-readable string."""
    if not suggestions:
        return "No runbook suggestions."
    lines = ["=== Runbook Suggestions ==="]
    for s in suggestions:
        lines.append(s.format())
        lines.append("")
    return "\n".join(lines).rstrip()
