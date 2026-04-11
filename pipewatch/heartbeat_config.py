"""Load heartbeat configuration from YAML."""

from __future__ import annotations

from pathlib import Path

import yaml

from pipewatch.heartbeat import HeartbeatConfig

_DEFAULT_PATH = "pipewatch-heartbeat.yaml"


def load_heartbeat_config(path: str = _DEFAULT_PATH) -> HeartbeatConfig:
    """Load HeartbeatConfig from *path*, returning defaults if missing."""
    p = Path(path)
    if not p.exists():
        return HeartbeatConfig()
    raw = yaml.safe_load(p.read_text()) or {}
    section = raw.get("heartbeat", {})
    return HeartbeatConfig(
        timeout_seconds=float(section.get("timeout_seconds", 60.0)),
        warning_seconds=float(section.get("warning_seconds", 30.0)),
    )
