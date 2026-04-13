"""Load rollup configuration from YAML."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import yaml


@dataclass
class RollupConfig:
    """Configuration for the rollup feature."""

    windows: List[str] = None  # e.g. ["1h", "6h", "24h"]
    enabled: bool = True

    def __post_init__(self) -> None:
        if self.windows is None:
            self.windows = ["1h", "24h"]


def load_rollup_config(path: str = "pipewatch-rollup.yaml") -> RollupConfig:
    """Load rollup config from *path*; return defaults if the file is absent."""
    p = Path(path)
    if not p.exists():
        return RollupConfig()
    raw = yaml.safe_load(p.read_text()) or {}
    rollup_section = raw.get("rollup", {})
    return RollupConfig(
        windows=rollup_section.get("windows", ["1h", "24h"]),
        enabled=rollup_section.get("enabled", True),
    )
