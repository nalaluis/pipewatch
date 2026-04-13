"""Configuration loader for the comparator module."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import yaml


@dataclass
class ComparatorConfig:
    """Settings that govern which pipelines are included in comparisons."""
    include_pipelines: List[str] = field(default_factory=list)  # empty = all
    exclude_pipelines: List[str] = field(default_factory=list)
    highlight_regressions: bool = True
    highlight_improvements: bool = True


def load_comparator_config(path: str = "pipewatch-comparator.yaml") -> ComparatorConfig:
    """Load comparator config from *path*, returning defaults when the file is absent."""
    p = Path(path)
    if not p.exists():
        return ComparatorConfig()
    with p.open() as fh:
        data = yaml.safe_load(fh) or {}
    return ComparatorConfig(
        include_pipelines=data.get("include_pipelines", []),
        exclude_pipelines=data.get("exclude_pipelines", []),
        highlight_regressions=data.get("highlight_regressions", True),
        highlight_improvements=data.get("highlight_improvements", True),
    )
