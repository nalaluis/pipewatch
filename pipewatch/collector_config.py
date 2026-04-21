"""Load CollectorConfig from YAML."""
from __future__ import annotations

import os
from typing import Any, Dict

import yaml

from pipewatch.collector import CollectorConfig
from pipewatch.metrics import PipelineStatus

_DEFAULT_PATH = "pipewatch-collector.yaml"


def load_collector_config(path: str = _DEFAULT_PATH) -> CollectorConfig:
    """Parse *path* and return a CollectorConfig (defaults on missing file)."""
    if not os.path.exists(path):
        return CollectorConfig()

    with open(path, "r", encoding="utf-8") as fh:
        data: Dict[str, Any] = yaml.safe_load(fh) or {}

    cfg = data.get("collector", {})

    default_status_raw = cfg.get("default_status", "healthy").upper()
    try:
        default_status = PipelineStatus[default_status_raw]
    except KeyError:
        default_status = PipelineStatus.HEALTHY

    return CollectorConfig(
        max_batch_size=int(cfg.get("max_batch_size", 50)),
        skip_unknown=bool(cfg.get("skip_unknown", False)),
        default_status=default_status,
    )
