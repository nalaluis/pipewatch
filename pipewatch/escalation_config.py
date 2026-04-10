"""Load escalation policy from YAML config."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

from pipewatch.alerts import AlertLevel
from pipewatch.escalation import EscalationPolicy

_DEFAULT_CONFIG_PATH = "pipewatch-escalation.yaml"


def load_escalation_config(path: str = _DEFAULT_CONFIG_PATH) -> EscalationPolicy:
    """Parse escalation policy from a YAML file; fall back to defaults if missing."""
    config_path = Path(path)
    if not config_path.exists():
        return EscalationPolicy()

    with config_path.open() as fh:
        raw: Dict[str, Any] = yaml.safe_load(fh) or {}

    escalation = raw.get("escalation", {})

    escalate_after = float(escalation.get("escalate_after_seconds", 300.0))
    max_escalations = int(escalation.get("max_escalations", 1))

    level_str: str = escalation.get("escalate_to", "critical").upper()
    try:
        escalate_to = AlertLevel[level_str]
    except KeyError:
        escalate_to = AlertLevel.CRITICAL

    return EscalationPolicy(
        escalate_after_seconds=escalate_after,
        escalate_to=escalate_to,
        max_escalations=max_escalations,
    )
