"""Load replay configuration from YAML."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import yaml


@dataclass
class ReplayConfig:
    """Configuration for pipeline replay behaviour."""
    pipelines: List[str] = field(default_factory=list)
    min_level: str = "WARNING"
    include_healthy: bool = True


def load_replay_config(path: str = "pipewatch-replay.yaml") -> ReplayConfig:
    """Load replay config from *path*, returning defaults if the file is absent."""
    p = Path(path)
    if not p.exists():
        return ReplayConfig()

    with p.open() as fh:
        raw = yaml.safe_load(fh) or {}

    replay_section = raw.get("replay", {})
    return ReplayConfig(
        pipelines=replay_section.get("pipelines", []),
        min_level=replay_section.get("min_level", "WARNING"),
        include_healthy=replay_section.get("include_healthy", True),
    )
