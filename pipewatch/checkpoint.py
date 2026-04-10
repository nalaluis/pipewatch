"""Checkpoint tracking for pipeline run state persistence."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional

DEFAULT_CHECKPOINT_DIR = ".pipewatch/checkpoints"


@dataclass
class Checkpoint:
    pipeline: str
    last_run_at: str          # ISO-8601 UTC
    last_status: str          # healthy / warning / critical
    run_count: int
    consecutive_failures: int


def _checkpoint_path(pipeline: str, directory: str = DEFAULT_CHECKPOINT_DIR) -> str:
    safe = pipeline.replace("/", "_").replace(" ", "_")
    return os.path.join(directory, f"{safe}.json")


def save_checkpoint(checkpoint: Checkpoint, directory: str = DEFAULT_CHECKPOINT_DIR) -> str:
    os.makedirs(directory, exist_ok=True)
    path = _checkpoint_path(checkpoint.pipeline, directory)
    with open(path, "w") as fh:
        json.dump(asdict(checkpoint), fh, indent=2)
    return path


def load_checkpoint(pipeline: str, directory: str = DEFAULT_CHECKPOINT_DIR) -> Optional[Checkpoint]:
    path = _checkpoint_path(pipeline, directory)
    if not os.path.exists(path):
        return None
    with open(path) as fh:
        data = json.load(fh)
    return Checkpoint(**data)


def update_checkpoint(pipeline: str, status: str, directory: str = DEFAULT_CHECKPOINT_DIR) -> Checkpoint:
    existing = load_checkpoint(pipeline, directory)
    now = datetime.now(timezone.utc).isoformat()
    if existing is None:
        cp = Checkpoint(
            pipeline=pipeline,
            last_run_at=now,
            last_status=status,
            run_count=1,
            consecutive_failures=1 if status == "critical" else 0,
        )
    else:
        consecutive = (
            existing.consecutive_failures + 1 if status == "critical" else 0
        )
        cp = Checkpoint(
            pipeline=pipeline,
            last_run_at=now,
            last_status=status,
            run_count=existing.run_count + 1,
            consecutive_failures=consecutive,
        )
    save_checkpoint(cp, directory)
    return cp


def clear_checkpoint(pipeline: str, directory: str = DEFAULT_CHECKPOINT_DIR) -> bool:
    path = _checkpoint_path(pipeline, directory)
    if os.path.exists(path):
        os.remove(path)
        return True
    return False
