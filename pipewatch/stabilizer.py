"""Stabilizer: suppress alerts until a condition persists for N consecutive checks."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

from pipewatch.alerts import Alert, AlertLevel


@dataclass
class StabilizerConfig:
    min_consecutive: int = 3  # how many consecutive hits before alert fires


@dataclass
class StabilizerEntry:
    pipeline: str
    alert_key: str
    consecutive: int = 0


# module-level state keyed by (pipeline, alert_key)
_state: Dict[str, StabilizerEntry] = {}


def _key(alert: Alert) -> str:
    return f"{alert.pipeline}::{alert.metric}"


def record_hit(alert: Alert) -> StabilizerEntry:
    """Increment the consecutive counter for this alert signal."""
    k = _key(alert)
    if k not in _state:
        _state[k] = StabilizerEntry(pipeline=alert.pipeline, alert_key=k)
    _state[k].consecutive += 1
    return _state[k]


def reset_hit(alert: Alert) -> None:
    """Reset the consecutive counter when the condition clears."""
    k = _key(alert)
    if k in _state:
        del _state[k]


def consecutive_count(alert: Alert) -> int:
    """Return how many consecutive hits have been recorded."""
    return _state.get(_key(alert), StabilizerEntry(pipeline="", alert_key="")).consecutive


def should_fire(alert: Alert, config: StabilizerConfig) -> bool:
    """Return True only when the alert has persisted for min_consecutive checks."""
    if alert.level == AlertLevel.OK:
        return False
    return consecutive_count(alert) >= config.min_consecutive


def apply_stabilizer(alerts: list[Alert], config: StabilizerConfig) -> list[Alert]:
    """Filter alerts, keeping only those that have persisted long enough."""
    result: list[Alert] = []
    for alert in alerts:
        if alert.level == AlertLevel.OK:
            reset_hit(alert)
            continue
        record_hit(alert)
        if should_fire(alert, config):
            result.append(alert)
    return result


def clear_state() -> None:
    """Reset all stabilizer state (useful for tests)."""
    _state.clear()
