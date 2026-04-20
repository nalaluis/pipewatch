"""Maps pipeline health signals to severity levels and human-readable summaries."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.health import HealthResult


@dataclass
class SignalEntry:
    pipeline: str
    level: AlertLevel
    reasons: List[str] = field(default_factory=list)
    alert_count: int = 0

    def __str__(self) -> str:
        badge = self.level.value.upper()
        summary = "; ".join(self.reasons) if self.reasons else "no issues"
        return f"[{badge}] {self.pipeline}: {summary} ({self.alert_count} alert(s))"


@dataclass
class SignalMap:
    entries: Dict[str, SignalEntry] = field(default_factory=dict)

    def get(self, pipeline: str) -> Optional[SignalEntry]:
        return self.entries.get(pipeline)

    def pipelines(self) -> List[str]:
        return list(self.entries.keys())

    def critical_pipelines(self) -> List[str]:
        return [
            name
            for name, entry in self.entries.items()
            if entry.level == AlertLevel.CRITICAL
        ]

    def warning_pipelines(self) -> List[str]:
        return [
            name
            for name, entry in self.entries.items()
            if entry.level == AlertLevel.WARNING
        ]


def build_signal_map(
    results: List[HealthResult],
    alerts: List[Alert],
) -> SignalMap:
    """Build a SignalMap from health results and active alerts."""
    alert_index: Dict[str, List[Alert]] = {}
    for alert in alerts:
        alert_index.setdefault(alert.pipeline, []).append(alert)

    entries: Dict[str, SignalEntry] = {}
    for result in results:
        pipeline = result.pipeline
        pipeline_alerts = alert_index.get(pipeline, [])

        level = AlertLevel.OK
        for alert in pipeline_alerts:
            if alert.level == AlertLevel.CRITICAL:
                level = AlertLevel.CRITICAL
                break
            if alert.level == AlertLevel.WARNING:
                level = AlertLevel.WARNING

        reasons = [a.message for a in pipeline_alerts]
        entries[pipeline] = SignalEntry(
            pipeline=pipeline,
            level=level,
            reasons=reasons,
            alert_count=len(pipeline_alerts),
        )

    return SignalMap(entries=entries)


def format_signal_map(signal_map: SignalMap) -> str:
    """Render a SignalMap as a multi-line string."""
    if not signal_map.entries:
        return "(no pipelines)"
    lines = [str(entry) for entry in signal_map.entries.values()]
    return "\n".join(lines)
