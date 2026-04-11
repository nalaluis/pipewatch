"""Load quota configuration from YAML."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

from pipewatch.quota import QuotaConfig

_DEFAULT_PATH = "pipewatch-quota.yaml"


def load_quota_config(path: str = _DEFAULT_PATH) -> QuotaConfig:
    """Load QuotaConfig from *path*, returning defaults if the file is absent."""
    p = Path(path)
    if not p.exists():
        return QuotaConfig()

    raw: Dict[str, Any] = yaml.safe_load(p.read_text()) or {}
    quota = raw.get("quota", {})

    return QuotaConfig(
        max_alerts=int(quota.get("max_alerts", 10)),
        window_seconds=int(quota.get("window_seconds", 3600)),
    )
