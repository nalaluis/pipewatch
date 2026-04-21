"""Configuration loader for the pipeline inspector."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import yaml


@dataclass
class InspectorConfig:
    include_score: bool = True
    include_trend: bool = True
    include_anomalies: bool = True
    pipelines: List[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.pipelines is None:
            self.pipelines = []


def load_inspector_config(path: str = "pipewatch-inspector.yaml") -> InspectorConfig:
    """Load inspector config from *path*; return defaults when file is absent."""
    p = Path(path)
    if not p.exists():
        return InspectorConfig()
    raw = yaml.safe_load(p.read_text()) or {}
    cfg = raw.get("inspector", {})
    return InspectorConfig(
        include_score=cfg.get("include_score", True),
        include_trend=cfg.get("include_trend", True),
        include_anomalies=cfg.get("include_anomalies", True),
        pipelines=cfg.get("pipelines", []),
    )
