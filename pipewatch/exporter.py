"""Export pipeline health reports to various formats (JSON, CSV)."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import asdict
from typing import List, Literal

from pipewatch.reporter import Report

ExportFormat = Literal["json", "csv"]


def export_json(report: Report, indent: int = 2) -> str:
    """Serialize a Report to a JSON string."""
    payload = {
        "timestamp": report.timestamp,
        "pipelines": [
            {
                "name": r.metric.name,
                "status": r.status.value,
                "failure_rate": r.metric.failure_rate,
                "throughput": r.metric.throughput,
                "violations": r.violations,
            }
            for r in report.results
        ],
        "alerts": [
            {
                "pipeline": a.pipeline,
                "level": a.level.value,
                "message": a.message,
            }
            for a in report.alerts
        ],
        "summary": asdict(report.summary) if report.summary else None,
    }
    return json.dumps(payload, indent=indent)


def export_csv(report: Report) -> str:
    """Serialize pipeline results in a Report to CSV."""
    output = io.StringIO()
    fieldnames = ["timestamp", "name", "status", "failure_rate", "throughput", "violations"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for r in report.results:
        writer.writerow(
            {
                "timestamp": report.timestamp,
                "name": r.metric.name,
                "status": r.status.value,
                "failure_rate": r.metric.failure_rate,
                "throughput": r.metric.throughput,
                "violations": "|".join(r.violations),
            }
        )
    return output.getvalue()


def export_report(report: Report, fmt: ExportFormat = "json") -> str:
    """Dispatch export based on requested format."""
    if fmt == "json":
        return export_json(report)
    if fmt == "csv":
        return export_csv(report)
    raise ValueError(f"Unsupported export format: {fmt!r}")
