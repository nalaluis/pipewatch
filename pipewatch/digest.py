"""Periodic digest summarising pipeline health across a reporting window."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.aggregator import PipelineSummary, aggregate
from pipewatch.alerts import Alert, AlertLevel
from pipewatch.health import HealthResult


@dataclass
class DigestEntry:
    pipeline: str
    status: str
    failure_rate: float
    throughput: float
    alert_count: int


@dataclass
class Digest:
    generated_at: str
    window_seconds: int
    summary: PipelineSummary
    entries: List[DigestEntry] = field(default_factory=list)
    top_alerts: List[str] = field(default_factory=list)


def _utcnow() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def build_digest(
    results: List[HealthResult],
    alerts: List[Alert],
    window_seconds: int = 300,
) -> Digest:
    """Build a digest from a list of health results and alerts."""
    summary = aggregate(results)
    alert_index: dict[str, int] = {}
    for a in alerts:
        alert_index[a.pipeline] = alert_index.get(a.pipeline, 0) + 1

    entries = [
        DigestEntry(
            pipeline=r.pipeline,
            status=r.status.value,
            failure_rate=r.failure_rate,
            throughput=r.throughput,
            alert_count=alert_index.get(r.pipeline, 0),
        )
        for r in results
    ]

    critical_alerts = [
        f"[{a.level.value.upper()}] {a.pipeline}: {a.message}"
        for a in alerts
        if a.level == AlertLevel.CRITICAL
    ]

    return Digest(
        generated_at=_utcnow(),
        window_seconds=window_seconds,
        summary=summary,
        entries=entries,
        top_alerts=critical_alerts[:5],
    )


def format_digest(digest: Digest) -> str:
    """Render a human-readable digest string."""
    lines: List[str] = [
        f"=== PipeWatch Digest ({digest.generated_at}) ===",
        f"Window : {digest.window_seconds}s",
        f"Pipelines: {digest.summary.total} total, "
        f"{digest.summary.critical_count} critical, "
        f"{digest.summary.warning_count} warning, "
        f"{digest.summary.healthy_count} healthy",
        f"Avg failure rate : {digest.summary.avg_failure_rate:.1%}",
        f"Avg throughput   : {digest.summary.avg_throughput:.1f} rec/s",
        "",
    ]
    if digest.entries:
        lines.append("Pipeline breakdown:")
        for e in digest.entries:
            alert_str = f" ({e.alert_count} alerts)" if e.alert_count else ""
            lines.append(
                f"  {e.pipeline:<30} {e.status:<10} "
                f"fail={e.failure_rate:.1%}  tput={e.throughput:.1f}{alert_str}"
            )
        lines.append("")
    if digest.top_alerts:
        lines.append("Top critical alerts:")
        for msg in digest.top_alerts:
            lines.append(f"  {msg}")
    return "\n".join(lines)
