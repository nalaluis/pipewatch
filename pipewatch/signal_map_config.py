"""Configuration loader for signal_map feature."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class SignalMapConfig:
    show_ok: bool = True
    max_reasons: int = 5
    output_format: str = "text"  # "text" or "json"


def load_signal_map_config(path: str = "pipewatch-signal-map.yaml") -> SignalMapConfig:
    """Load SignalMapConfig from a YAML file, returning defaults if missing."""
    p = Path(path)
    if not p.exists():
        return SignalMapConfig()
    try:
        raw = yaml.safe_load(p.read_text()) or {}
    except yaml.YAMLError:
        return SignalMapConfig()
    cfg = raw.get("signal_map", {})
    return SignalMapConfig(
        show_ok=bool(cfg.get("show_ok", True)),
        max_reasons=int(cfg.get("max_reasons", 5)),
        output_format=str(cfg.get("output_format", "text")),
    )
