"""Load throttle configuration from YAML."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

from pipewatch.alerts import AlertLevel
from pipewatch.throttle import ThrottleConfig

_DEFAULT_PATH = Path("pipewatch-throttle.yaml")


def load_throttle_config(path: str | Path = _DEFAULT_PATH) -> ThrottleConfig:
    """Parse a YAML file and return a :class:`ThrottleConfig`.

    Falls back to defaults when the file is missing or the key is absent.
    """
    p = Path(path)
    if not p.exists():
        return ThrottleConfig()

    with p.open() as fh:
        raw: Dict[str, Any] = yaml.safe_load(fh) or {}

    throttle_raw = raw.get("throttle", {})
    cooldown = int(throttle_raw.get("cooldown_seconds", 300))

    level_str = throttle_raw.get("min_level", "warning").upper()
    try:
        min_level = AlertLevel[level_str]
    except KeyError:
        min_level = AlertLevel.WARNING

    return ThrottleConfig(cooldown_seconds=cooldown, min_level=min_level)
