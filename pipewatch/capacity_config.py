"""Load CapacityConfig from YAML."""
from __future__ import annotations

import os
from typing import Any, Dict

import yaml

from pipewatch.capacity import CapacityConfig

_DEFAULT_PATH = "pipewatch-capacity.yaml"


def load_capacity_config(path: str = _DEFAULT_PATH) -> CapacityConfig:
    """Parse *path* and return a :class:`CapacityConfig`.

    Missing file or empty YAML returns defaults.
    """
    if not os.path.exists(path):
        return CapacityConfig()

    with open(path, "r") as fh:
        data: Any = yaml.safe_load(fh)

    if not isinstance(data, dict):
        return CapacityConfig()

    cfg_data: Dict[str, Any] = data.get("capacity", data)

    return CapacityConfig(
        max_throughput=float(cfg_data.get("max_throughput", 1000.0)),
        warn_pct=float(cfg_data.get("warn_pct", 0.80)),
        critical_pct=float(cfg_data.get("critical_pct", 0.95)),
        min_throughput=float(cfg_data.get("min_throughput", 0.0)),
        enabled=bool(cfg_data.get("enabled", True)),
    )
