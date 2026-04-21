"""Load DebouncerConfig from YAML."""
from __future__ import annotations

from pathlib import Path

import yaml

from pipewatch.debouncer import DebouncerConfig

_DEFAULT_PATH = "pipewatch-debouncer.yaml"


def load_debouncer_config(path: str = _DEFAULT_PATH) -> DebouncerConfig:
    """Parse debouncer settings from *path*; return defaults if missing."""
    p = Path(path)
    if not p.exists():
        return DebouncerConfig()
    raw = yaml.safe_load(p.read_text()) or {}
    debouncer = raw.get("debouncer", {})
    return DebouncerConfig(
        min_duration_seconds=float(debouncer.get("min_duration_seconds", 30.0)),
        enabled=bool(debouncer.get("enabled", True)),
    )
