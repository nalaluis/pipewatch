"""Load dispatcher rules from YAML config."""
from __future__ import annotations

from pathlib import Path
from typing import List

import yaml

from pipewatch.alerts import AlertLevel
from pipewatch.dispatcher import DispatchRule

_DEFAULT_PATH = "pipewatch-dispatcher.yaml"


def _parse_level(raw: str) -> AlertLevel:
    try:
        return AlertLevel[raw.upper()]
    except KeyError:
        return AlertLevel.WARNING


def load_dispatcher_rules(path: str = _DEFAULT_PATH) -> List[DispatchRule]:
    """Load dispatch rules from a YAML file.

    Expected format:
        rules:
          - pipeline: my_pipeline   # optional
            min_level: critical
            handler: pagerduty
          - min_level: warning
            handler: slack
    """
    p = Path(path)
    if not p.exists():
        return []
    raw = yaml.safe_load(p.read_text()) or {}
    rules_raw = raw.get("rules", []) or []
    rules: List[DispatchRule] = []
    for entry in rules_raw:
        if not isinstance(entry, dict):
            continue
        handler_name = entry.get("handler", "")
        if not handler_name:
            continue
        rules.append(
            DispatchRule(
                pipeline=entry.get("pipeline"),
                min_level=_parse_level(entry.get("min_level", "warning")),
                handler_name=handler_name,
            )
        )
    return rules
