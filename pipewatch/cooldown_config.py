"""Load cooldown configuration from YAML."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

from pipewatch.cooldown import CooldownConfig

_DEFAULT_PATH = "pipewatch-cooldown.yaml"


def load_cooldown_config(path: str = _DEFAULT_PATH) -> CooldownConfig:
    """Load CooldownConfig from *path*; return defaults if file is missing."""
    p = Path(path)
    if not p.exists():
        return CooldownConfig()

    raw: Any = yaml.safe_load(p.read_text()) or {}
    if not isinstance(raw, dict):
        return CooldownConfig()

    cooldown_section: Dict[str, Any] = raw.get("cooldown", {})
    default_seconds = float(cooldown_section.get("default_seconds", 300.0))
    per_pipeline: Dict[str, float] = {
        k: float(v)
        for k, v in cooldown_section.get("per_pipeline", {}).items()
    }
    return CooldownConfig(default_seconds=default_seconds, per_pipeline=per_pipeline)
