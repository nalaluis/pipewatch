"""Load eviction configuration from YAML."""

from __future__ import annotations

import os
from typing import Any, Dict

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore

from pipewatch.eviction import EvictionConfig

_DEFAULT_PATH = "pipewatch-eviction.yaml"


def load_eviction_config(path: str = _DEFAULT_PATH) -> EvictionConfig:
    """Load EvictionConfig from a YAML file, returning defaults if missing."""
    if not os.path.exists(path):
        return EvictionConfig()

    with open(path, "r") as fh:
        raw: Dict[str, Any] = yaml.safe_load(fh) or {}

    eviction = raw.get("eviction", {})
    return EvictionConfig(
        max_age_seconds=float(eviction.get("max_age_seconds", 3600.0)),
        max_entries=int(eviction.get("max_entries", 500)),
        enabled=bool(eviction.get("enabled", True)),
    )
