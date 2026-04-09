"""Load circuit breaker configuration from YAML."""

from pathlib import Path
from typing import Any, Dict

import yaml

from pipewatch.circuit_breaker import CircuitBreakerConfig

_DEFAULT_CONFIG_PATH = "pipewatch-circuit-breaker.yaml"


def load_circuit_breaker_config(path: str = _DEFAULT_CONFIG_PATH) -> CircuitBreakerConfig:
    """Load CircuitBreakerConfig from a YAML file, returning defaults if missing."""
    p = Path(path)
    if not p.exists():
        return CircuitBreakerConfig()
    raw = yaml.safe_load(p.read_text()) or {}
    section: Dict[str, Any] = raw.get("circuit_breaker", {})
    return CircuitBreakerConfig(
        failure_threshold=int(section.get("failure_threshold", 5)),
        recovery_timeout=float(section.get("recovery_timeout", 300.0)),
        success_threshold=int(section.get("success_threshold", 2)),
    )
