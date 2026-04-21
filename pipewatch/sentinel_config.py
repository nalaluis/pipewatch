"""Load SentinelConfig from YAML."""
from __future__ import annotations

import os
from typing import Any, Dict

import yaml

from pipewatch.sentinel import SentinelConfig

_DEFAULT_PATH = "pipewatch-sentinel.yaml"


def load_sentinel_config(path: str = _DEFAULT_PATH) -> SentinelConfig:
    """Load sentinel configuration from *path*, returning defaults if missing."""
    if not os.path.exists(path):
        return SentinelConfig()
    with open(path, "r") as fh:
        data: Dict[str, Any] = yaml.safe_load(fh) or {}
    sentinel = data.get("sentinel", {})
    return SentinelConfig(
        threshold=int(sentinel.get("threshold", 3)),
        window_seconds=float(sentinel.get("window_seconds", 300)),
        cooldown_seconds=float(sentinel.get("cooldown_seconds", 60)),
    )
