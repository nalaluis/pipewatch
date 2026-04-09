"""Builds and renders pipeline health reports."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List

from pipewatch.health import HealthResult
from pipewatch.alerts import Alert, AlertLevel
from pipewatch.aggregator import PipelineSummary, aggregate, format_summary


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class Report:
    timestamp: str
    results: List[HealthResult]
    alerts: List[Alert]
    summary: PipelineSummary = field(default_factory=PipelineSummary)


def build_report(results: List[HealthResult], alerts: List[Alert]) -> Report:
    """Assemble a Report from health results and alerts."""
    summary = aggregate(results)
    return Report(
        timestamp=_timestamp(),
        results=results,
        alerts=alerts,
        summary=summary,
    )


def render_text(report: Report) -> str:
    """Render a Report as a plain-text string suitable for CLI output."""
    lines = [f"PipeWatch Report  [{report.timestamp}]"]
    lines.append("-" * 50)

    for result in report.results:
        m = result.metric
        lines.append(
            f"  [{result.status.value.upper():8s}] {m.pipeline_name}"
            f"  failure={m.failure_rate:.2%}  throughput={m.throughput:.1f} rec/s"
        )
        for v in result.violations:
            lines.append(f"             ↳ {v}")

    if report.alerts:
        lines.append("")
        lines.append("Alerts:")
        for alert in report.alerts:
            lines.append(f"  [{alert.level.value.upper():8s}] {alert.pipeline}: {alert.message}")

    lines.append("")
    lines.append(format_summary(report.summary))
    return "\n".join(lines)


def emit_report(report: Report) -> None:
    """Print the rendered report to stdout."""
    print(render_text(report))


def has_critical(report: Report) -> bool:
    """Return True if any alert in the report is CRITICAL level."""
    return any(a.level == AlertLevel.CRITICAL for a in report.alerts)


def has_warnings(report: Report) -> bool:
    """Return True if any alert in the report is WARNING level."""
    return any(a.level == AlertLevel.WARNING for a in report.alerts)
