"""Load compactor configuration from YAML."""

from __future__ import annotations

import os
from typing import Any, Dict

import yaml

from pipewatch.compactor import CompactorConfig

_DEFAULT_CONFIG_PATH = "pipewatch-compactor.yaml"


def load_compactor_config(path: str = _DEFAULT_CONFIG_PATH) -> CompactorConfig:
    """Load CompactorConfig from a YAML file, returning defaults on missing/empty."""
    if not os.path.exists(path):
        return CompactorConfig()
    with open(path, "r") as fh:
        data: Dict[str, Any] = yaml.safe_load(fh) or {}
    return CompactorConfig(
        retention_seconds=float(data.get("retention_seconds", 86400.0)),
        dry_run=bool(data.get("dry_run", False)),
    )
