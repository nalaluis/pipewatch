"""Load ScorerConfig from YAML."""
from __future__ import annotations

from pathlib import Path

import yaml

from pipewatch.scorer import ScorerConfig

_DEFAULT_PATH = "pipewatch-scorer.yaml"


def load_scorer_config(path: str = _DEFAULT_PATH) -> ScorerConfig:
    """Return a ScorerConfig from *path*; fall back to defaults if missing."""
    p = Path(path)
    if not p.exists():
        return ScorerConfig()

    with p.open() as fh:
        data = yaml.safe_load(fh) or {}

    scorer_data = data.get("scorer", data)  # support both top-level and nested
    return ScorerConfig(
        failure_rate_weight=float(
            scorer_data.get("failure_rate_weight", 0.5)
        ),
        throughput_weight=float(
            scorer_data.get("throughput_weight", 0.3)
        ),
        status_weight=float(
            scorer_data.get("status_weight", 0.2)
        ),
        throughput_baseline=float(
            scorer_data.get("throughput_baseline", 100.0)
        ),
    )
