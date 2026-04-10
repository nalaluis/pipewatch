"""Load anomaly detection configuration from YAML."""

from __future__ import annotations

import os
from typing import Any, Dict

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore

from pipewatch.anomaly import AnomalyConfig

_DEFAULT_PATH = "pipewatch-anomaly.yaml"


def load_anomaly_config(path: str = _DEFAULT_PATH) -> AnomalyConfig:
    """Load AnomalyConfig from a YAML file, returning defaults if missing."""
    if not os.path.exists(path):
        return AnomalyConfig()

    with open(path, "r") as fh:
        raw: Dict[str, Any] = yaml.safe_load(fh) or {}

    section = raw.get("anomaly", {})
    return AnomalyConfig(
        z_score_threshold=float(section.get("z_score_threshold", 2.5)),
        min_history=int(section.get("min_history", 3)),
    )
