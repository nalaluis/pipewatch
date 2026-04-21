"""Load tracer configuration from YAML."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore


@dataclass
class TracerConfig:
    enabled: bool = True
    max_spans: int = 500
    slow_threshold_ms: float = 1000.0


def load_tracer_config(path: str = "pipewatch-tracer.yaml") -> TracerConfig:
    """Load :class:`TracerConfig` from *path*.

    Returns defaults when the file is missing or empty.
    """
    if not os.path.exists(path):
        return TracerConfig()

    with open(path, "r") as fh:
        raw: Dict[str, Any] = (yaml.safe_load(fh) or {}) if yaml else {}

    tracer_section: Dict[str, Any] = raw.get("tracer", raw)

    return TracerConfig(
        enabled=bool(tracer_section.get("enabled", True)),
        max_spans=int(tracer_section.get("max_spans", 500)),
        slow_threshold_ms=float(tracer_section.get("slow_threshold_ms", 1000.0)),
    )
