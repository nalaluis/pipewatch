"""Audit log: records pipeline evaluation events for traceability."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.metrics import PipelineMetric
from pipewatch.health import HealthResult
from pipewatch.alerts import Alert

_AUDIT_DIR = os.path.join(".pipewatch", "audit")


@dataclass
class AuditEntry:
    pipeline: str
    timestamp: str
    status: str
    failure_rate: float
    throughput: float
    alert_levels: List[str] = field(default_factory=list)
    notes: str = ""


def _audit_path(pipeline: str) -> str:
    os.makedirs(_AUDIT_DIR, exist_ok=True)
    safe = pipeline.replace("/", "_").replace(" ", "_")
    return os.path.join(_AUDIT_DIR, f"{safe}.jsonl")


def record_audit(
    metric: PipelineMetric,
    result: HealthResult,
    alerts: Optional[List[Alert]] = None,
    notes: str = "",
) -> AuditEntry:
    """Append an audit entry for a pipeline evaluation."""
    alert_levels = [a.level.value for a in (alerts or [])]
    entry = AuditEntry(
        pipeline=metric.pipeline,
        timestamp=datetime.now(timezone.utc).isoformat(),
        status=result.status.value,
        failure_rate=metric.failure_rate,
        throughput=metric.throughput,
        alert_levels=alert_levels,
        notes=notes,
    )
    path = _audit_path(metric.pipeline)
    with open(path, "a") as fh:
        fh.write(json.dumps(entry.__dict__) + "\n")
    return entry


def load_audit(pipeline: str) -> List[AuditEntry]:
    """Load all audit entries for a pipeline."""
    path = _audit_path(pipeline)
    if not os.path.exists(path):
        return []
    entries: List[AuditEntry] = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                data = json.loads(line)
                entries.append(AuditEntry(**data))
    return entries


def clear_audit(pipeline: str) -> None:
    """Remove the audit log for a pipeline."""
    path = _audit_path(pipeline)
    if os.path.exists(path):
        os.remove(path)
