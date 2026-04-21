"""Load forecaster configuration from YAML."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Any

import yaml


@dataclass
class ForecasterConfig:
    steps_ahead: int = 1
    min_samples: int = 3
    max_samples: int = 20
    enabled: bool = True


def load_forecaster_config(path: str = "pipewatch-forecaster.yaml") -> ForecasterConfig:
    try:
        with open(path) as fh:
            data: Dict[str, Any] = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        return ForecasterConfig()

    return ForecasterConfig(
        steps_ahead=int(data.get("steps_ahead", 1)),
        min_samples=int(data.get("min_samples", 3)),
        max_samples=int(data.get("max_samples", 20)),
        enabled=bool(data.get("enabled", True)),
    )
