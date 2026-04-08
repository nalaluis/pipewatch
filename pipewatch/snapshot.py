"""Snapshot persistence: save and load pipeline metric snapshots to disk."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus


DEFAULT_SNAPSHOT_DIR = ".pipewatch_snapshots"


def _snapshot_path(directory: str, pipeline_name: str) -> str:
    safe_name = pipeline_name.replace("/", "_").replace(" ", "_")
    return os.path.join(directory, f"{safe_name}.json")


def save_snapshot(metric: PipelineMetric, directory: str = DEFAULT_SNAPSHOT_DIR) -> str:
    """Persist a PipelineMetric to a JSON file. Returns the file path written."""
    os.makedirs(directory, exist_ok=True)
    path = _snapshot_path(directory, metric.pipeline_name)
    payload = {
        "pipeline_name": metric.pipeline_name,
        "records_processed": metric.records_processed,
        "records_failed": metric.records_failed,
        "duration_seconds": metric.duration_seconds,
        "status": metric.status.value,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    return path


def load_snapshot(pipeline_name: str, directory: str = DEFAULT_SNAPSHOT_DIR) -> Optional[PipelineMetric]:
    """Load the most recent snapshot for a pipeline. Returns None if not found."""
    path = _snapshot_path(directory, pipeline_name)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    return PipelineMetric(
        pipeline_name=data["pipeline_name"],
        records_processed=data["records_processed"],
        records_failed=data["records_failed"],
        duration_seconds=data["duration_seconds"],
        status=PipelineStatus(data["status"]),
    )


def list_snapshots(directory: str = DEFAULT_SNAPSHOT_DIR) -> List[str]:
    """Return pipeline names for which snapshots exist."""
    if not os.path.isdir(directory):
        return []
    names = []
    for fname in os.listdir(directory):
        if fname.endswith(".json"):
            names.append(fname[:-5])
    return sorted(names)
