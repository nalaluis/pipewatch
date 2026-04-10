"""Load audit configuration from YAML."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

import yaml


@dataclass
class AuditConfig:
    enabled: bool = True
    audit_dir: str = ".pipewatch/audit"
    max_entries_per_pipeline: Optional[int] = None
    include_notes: bool = False


def load_audit_config(path: str = "pipewatch-audit.yaml") -> AuditConfig:
    """Load audit config from YAML, returning defaults if file is missing."""
    if not os.path.exists(path):
        return AuditConfig()
    with open(path) as fh:
        raw = yaml.safe_load(fh) or {}
    audit = raw.get("audit", {})
    return AuditConfig(
        enabled=audit.get("enabled", True),
        audit_dir=audit.get("audit_dir", ".pipewatch/audit"),
        max_entries_per_pipeline=audit.get("max_entries_per_pipeline"),
        include_notes=audit.get("include_notes", False),
    )
