"""Silencer: temporarily mute alerts for specific pipelines or alert levels."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.alerts import Alert, AlertLevel


@dataclass
class SilenceRule:
    pipeline: str  # exact name or glob pattern
    level: Optional[AlertLevel] = None  # None means all levels
    until: Optional[datetime] = None  # None means indefinite
    reason: str = ""


@dataclass
class SilencerState:
    _rules: List[SilenceRule] = field(default_factory=list)

    def add_rule(self, rule: SilenceRule) -> None:
        self._rules.append(rule)

    def remove_rule(self, pipeline: str) -> int:
        before = len(self._rules)
        self._rules = [r for r in self._rules if r.pipeline != pipeline]
        return before - len(self._rules)

    def active_rules(self, now: Optional[datetime] = None) -> List[SilenceRule]:
        now = now or datetime.now(timezone.utc)
        return [
            r for r in self._rules
            if r.until is None or r.until > now
        ]

    def clear_expired(self, now: Optional[datetime] = None) -> int:
        now = now or datetime.now(timezone.utc)
        before = len(self._rules)
        self._rules = self.active_rules(now)
        return before - len(self._rules)


_state = SilencerState()


def _matches(rule: SilenceRule, alert: Alert) -> bool:
    import fnmatch
    name_match = fnmatch.fnmatch(alert.pipeline, rule.pipeline)
    level_match = rule.level is None or rule.level == alert.level
    return name_match and level_match


def should_silence(alert: Alert, now: Optional[datetime] = None) -> bool:
    """Return True if the alert should be silenced based on active rules."""
    active = _state.active_rules(now)
    return any(_matches(r, alert) for r in active)


def apply_silencer(alerts: List[Alert], now: Optional[datetime] = None) -> List[Alert]:
    """Filter out silenced alerts, returning only those that should fire."""
    return [a for a in alerts if not should_silence(a, now)]


def get_state() -> SilencerState:
    return _state


def reset_state() -> None:
    global _state
    _state = SilencerState()
