"""Load and validate pipewatch configuration from a YAML file."""

from pathlib import Path
from typing import Any, Dict

import yaml

from pipewatch.health import HealthThresholds

DEFAULT_CONFIG_PATH = Path("pipewatch.yaml")


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    """Load raw configuration dictionary from a YAML file."""
    if not path.exists():
        return {}
    with path.open("r") as fh:
        data = yaml.safe_load(fh) or {}
    return data


def parse_thresholds(config: Dict[str, Any]) -> HealthThresholds:
    """Extract threshold settings from config dict, falling back to defaults."""
    raw = config.get("thresholds", {})
    return HealthThresholds(
        max_failure_rate=float(raw.get("max_failure_rate", 0.05)),
        min_throughput=float(raw.get("min_throughput", 10.0)),
        max_duration_seconds=float(raw.get("max_duration_seconds", 3600.0)),
        warning_failure_rate=float(raw.get("warning_failure_rate", 0.01)),
        warning_throughput=float(raw.get("warning_throughput", 50.0)),
    )


def get_pipelines(config: Dict[str, Any]) -> list[str]:
    """Return list of pipeline IDs to monitor."""
    return config.get("pipelines", [])
