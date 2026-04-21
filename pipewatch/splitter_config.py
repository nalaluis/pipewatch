"""Load splitter rules from YAML configuration."""
from __future__ import annotations

from pathlib import Path
from typing import List

import yaml

from pipewatch.metrics import PipelineStatus
from pipewatch.splitter import SplitRule

_DEFAULT_PATH = "pipewatch-splitter.yaml"


def _parse_status(value: str) -> PipelineStatus:
    mapping = {
        "healthy": PipelineStatus.HEALTHY,
        "warning": PipelineStatus.WARNING,
        "critical": PipelineStatus.CRITICAL,
    }
    return mapping.get(value.lower(), PipelineStatus.HEALTHY)


def load_splitter_rules(path: str = _DEFAULT_PATH) -> List[SplitRule]:
    """Parse splitter rules from *path*; returns empty list if file is missing."""
    p = Path(path)
    if not p.exists():
        return []
    raw = yaml.safe_load(p.read_text()) or {}
    rules_raw = raw.get("rules", []) or []
    rules: List[SplitRule] = []
    for entry in rules_raw:
        status_raw = entry.get("status")
        rules.append(
            SplitRule(
                bucket=entry["bucket"],
                pattern=entry.get("pattern"),
                status=_parse_status(status_raw) if status_raw else None,
                min_failure_rate=entry.get("min_failure_rate"),
                max_failure_rate=entry.get("max_failure_rate"),
            )
        )
    return rules
