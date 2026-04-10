"""Load sampler configuration from YAML."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

from pipewatch.sampler import SamplerConfig

_DEFAULT_PATH = "pipewatch-sampler.yaml"


def load_sampler_config(path: str = _DEFAULT_PATH) -> SamplerConfig:
    """Load SamplerConfig from a YAML file. Returns defaults if file is missing."""
    p = Path(path)
    if not p.exists():
        return SamplerConfig()

    with p.open() as fh:
        data: Dict[str, Any] = yaml.safe_load(fh) or {}

    sampler_data = data.get("sampler", {})
    return SamplerConfig(
        window_size=int(sampler_data.get("window_size", 10)),
        min_samples=int(sampler_data.get("min_samples", 2)),
    )
