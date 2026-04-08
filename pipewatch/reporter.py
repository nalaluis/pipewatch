"""Reporter module: formats and outputs pipeline health summaries to stdout or file."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from typing import IO, List, Optional

from pipewatch.alerts import Alert, AlertLevel
from pipewatch.health import HealthResult
from pipewatch.metrics import PipelineMetric, failure_rate, throughput, to_dict


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_report(
    pipeline_name: str,
    metric: PipelineMetric,
    result: HealthResult,
    alerts: List[Alert],
) -> dict:
    """Assemble a structured report dict for a single pipeline."""
    return {
        "pipeline": pipeline_name,
        "timestamp": _timestamp(),
        "metrics": to_dict(metric),
        "status": result.status.value,
        "healthy": result.healthy,
        "violations": result.violations,
        "alerts": [
            {
                "level": a.level.value,
                "message": a.message,
                "metric": a.metric,
                "value": a.value,
            }
            for a in alerts
        ],
    }


def render_text(report: dict) -> str:
    """Render a human-readable text summary of the report."""
    lines = [
        f"[{report['timestamp']}] Pipeline: {report['pipeline']}",
        f"  Status  : {report['status']}  |  Healthy: {report['healthy']}",
        f"  Metrics : failure_rate={report['metrics']['failure_rate']:.2%}  "
        f"throughput={report['metrics']['throughput']:.1f} rec/s",
    ]
    if report["violations"]:
        lines.append("  Violations:")
        for v in report["violations"]:
            lines.append(f"    - {v}")
    if report["alerts"]:
        lines.append("  Alerts:")
        for a in report["alerts"]:
            lines.append(f"    [{a['level'].upper()}] {a['message']}")
    return "\n".join(lines)


def emit_report(
    report: dict,
    fmt: str = "text",
    output: Optional[IO] = None,
) -> None:
    """Write the report to *output* (defaults to stdout) in *fmt* format."""
    if output is None:
        output = sys.stdout
    if fmt == "json":
        output.write(json.dumps(report, indent=2) + "\n")
    else:
        output.write(render_text(report) + "\n")
