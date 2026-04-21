"""Load remediation configuration from YAML."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

import yaml


@dataclass
class RemediationConfig:
    """Global remediation settings."""
    enabled: bool = True
    # violation keywords that should trigger remediation hints
    tracked_violations: List[str] = field(default_factory=lambda: ["failure_rate", "throughput"])
    # per-pipeline overrides: pipeline_name -> list of extra action titles to suppress
    suppressed_actions: Dict[str, List[str]] = field(default_factory=dict)


def load_remediation_config(path: str = "pipewatch-remediation.yaml") -> RemediationConfig:
    """Load remediation config, returning defaults when file is absent."""
    p = Path(path)
    if not p.exists():
        return RemediationConfig()
    with p.open() as fh:
        data = yaml.safe_load(fh) or {}
    return RemediationConfig(
        enabled=data.get("enabled", True),
        tracked_violations=data.get("tracked_violations", ["failure_rate", "throughput"]),
        suppressed_actions=data.get("suppressed_actions", {}),
    )
