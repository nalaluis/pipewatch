"""Load rate limiter configuration from YAML."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore

from pipewatch.rate_limiter import RateLimiterConfig

_DEFAULT_CONFIG_PATH = "pipewatch-rate-limiter.yaml"


def load_rate_limiter_config(path: str = _DEFAULT_CONFIG_PATH) -> RateLimiterConfig:
    """Load RateLimiterConfig from a YAML file.

    Falls back to defaults if the file is missing or empty.
    """
    p = Path(path)
    if not p.exists():
        return RateLimiterConfig()

    if yaml is None:  # pragma: no cover
        return RateLimiterConfig()

    raw: Any = yaml.safe_load(p.read_text())
    if not isinstance(raw, dict):
        return RateLimiterConfig()

    data: Dict[str, Any] = raw.get("rate_limiter", {})
    return RateLimiterConfig(
        min_interval_seconds=float(data.get("min_interval_seconds", 60.0)),
        per_pipeline=bool(data.get("per_pipeline", True)),
    )
