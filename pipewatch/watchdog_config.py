"""Load watchdog configuration from YAML."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

from pipewatch.watchdog import WatchdogConfig

_DEFAULT_PATH = "pipewatch-watchdog.yaml"


def load_watchdog_config(path: str = _DEFAULT_PATH) -> WatchdogConfig:
    """Load WatchdogConfig from *path*; return defaults if file is missing."""
    p = Path(path)
    if not p.exists():
        return WatchdogConfig()
    with p.open() as fh:
        data: Dict[str, Any] = yaml.safe_load(fh) or {}
    watchdog_data = data.get("watchdog", {})
    return WatchdogConfig(
        stale_after_seconds=int(watchdog_data.get("stale_after_seconds", 300)),
        enabled=bool(watchdog_data.get("enabled", True)),
    )
