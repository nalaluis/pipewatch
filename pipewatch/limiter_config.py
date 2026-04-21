"""Load LimiterConfig from YAML."""
from __future__ import annotations

from pathlib import Path

import yaml

from pipewatch.alerts import AlertLevel
from pipewatch.limiter import LimiterConfig

_DEFAULT_PATH = "pipewatch-limiter.yaml"


def load_limiter_config(path: str = _DEFAULT_PATH) -> LimiterConfig:
    """Load limiter configuration from *path*.

    Missing or empty files return defaults.
    """
    p = Path(path)
    if not p.exists():
        return LimiterConfig()
    raw = yaml.safe_load(p.read_text()) or {}
    data = raw.get("limiter", raw)
    if not isinstance(data, dict):
        return LimiterConfig()

    min_level_raw = data.get("min_level", "warning")
    try:
        min_level = AlertLevel[min_level_raw.upper()]
    except (KeyError, AttributeError):
        min_level = AlertLevel.WARNING

    return LimiterConfig(
        max_alerts_per_window=int(data.get("max_alerts_per_window", 5)),
        window_seconds=int(data.get("window_seconds", 300)),
        min_level=min_level,
    )
