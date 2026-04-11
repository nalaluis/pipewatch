"""Load budget configuration from YAML."""
from __future__ import annotations

import os
from typing import Any, Dict

import yaml

from pipewatch.budget import BudgetConfig

_DEFAULT_PATH = "pipewatch-budget.yaml"


def load_budget_config(path: str = _DEFAULT_PATH) -> BudgetConfig:
    """Load BudgetConfig from *path*; return defaults if file is missing."""
    if not os.path.exists(path):
        return BudgetConfig()
    with open(path, "r") as fh:
        data: Dict[str, Any] = yaml.safe_load(fh) or {}
    budget = data.get("budget", {})
    return BudgetConfig(
        max_alerts=int(budget.get("max_alerts", 50)),
        window_seconds=int(budget.get("window_seconds", 3600)),
        per_pipeline=bool(budget.get("per_pipeline", False)),
    )
