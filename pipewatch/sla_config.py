"""Load SLA configuration from YAML."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore

from pipewatch.sla import SLAConfig

_DEFAULT_PATH = "pipewatch-sla.yaml"


def load_sla_config(path: str = _DEFAULT_PATH) -> SLAConfig:
    """Load SLAConfig from a YAML file, returning defaults if absent."""
    p = Path(path)
    if not p.exists():
        return SLAConfig()

    if yaml is None:  # pragma: no cover
        raise RuntimeError("PyYAML is required to load SLA config")

    raw: Dict[str, Any] = yaml.safe_load(p.read_text()) or {}
    sla_section: Dict[str, Any] = raw.get("sla", {})

    return SLAConfig(
        max_failure_rate=float(sla_section.get("max_failure_rate", 0.05)),
        min_throughput=float(sla_section.get("min_throughput", 1.0)),
        max_downtime_seconds=float(sla_section.get("max_downtime_seconds", 300.0)),
    )
