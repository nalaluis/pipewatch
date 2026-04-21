"""Load mirror configuration from YAML."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore


@dataclass
class MirrorConfig:
    left_env: str = "production"
    right_env: str = "staging"
    left_config: str = "pipewatch.yaml"
    right_config: str = "pipewatch-staging.yaml"
    fail_on_divergence: bool = False


def load_mirror_config(path: str = "pipewatch-mirror.yaml") -> MirrorConfig:
    """Load MirrorConfig from *path*; return defaults when file is absent."""
    p = Path(path)
    if not p.exists():
        return MirrorConfig()
    with p.open() as fh:
        data = yaml.safe_load(fh) or {}
    return MirrorConfig(
        left_env=data.get("left_env", "production"),
        right_env=data.get("right_env", "staging"),
        left_config=data.get("left_config", "pipewatch.yaml"),
        right_config=data.get("right_config", "pipewatch-staging.yaml"),
        fail_on_divergence=bool(data.get("fail_on_divergence", False)),
    )
