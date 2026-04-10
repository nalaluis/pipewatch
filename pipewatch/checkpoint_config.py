"""Load checkpoint configuration from YAML."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore

DEFAULT_CONFIG_PATH = "pipewatch-checkpoint.yaml"


@dataclass
class CheckpointConfig:
    enabled: bool = True
    directory: str = ".pipewatch/checkpoints"
    max_consecutive_failures: int = 3


def load_checkpoint_config(path: str = DEFAULT_CONFIG_PATH) -> CheckpointConfig:
    if yaml is None or not os.path.exists(path):
        return CheckpointConfig()
    with open(path) as fh:
        raw = yaml.safe_load(fh) or {}
    section = raw.get("checkpoint", {})
    return CheckpointConfig(
        enabled=section.get("enabled", True),
        directory=section.get("directory", ".pipewatch/checkpoints"),
        max_consecutive_failures=section.get("max_consecutive_failures", 3),
    )
