"""Alert escalation policy: promote alert level after repeated violations."""

from __future__ import annotations

from dataclasses import dataclass, field
from time import time
from typing import Dict, Optional

from pipewatch.alerts import Alert, AlertLevel


@dataclass
class EscalationPolicy:
    """Configuration for when to escalate an alert to a higher level."""
    escalate_after_seconds: float = 300.0   # 5 minutes
    escalate_to: AlertLevel = AlertLevel.CRITICAL
    max_escalations: int = 1


@dataclass
class EscalationEntry:
    first_seen: float = field(default_factory=time)
    escalation_count: int = 0


@dataclass
class EscalationState:
    _entries: Dict[str, EscalationEntry] = field(default_factory=dict)

    def _key(self, alert: Alert) -> str:
        return f"{alert.pipeline}::{alert.metric}"

    def record(self, alert: Alert) -> None:
        key = self._key(alert)
        if key not in self._entries:
            self._entries[key] = EscalationEntry()

    def reset(self, alert: Alert) -> None:
        self._entries.pop(self._key(alert), None)

    def get(self, alert: Alert) -> Optional[EscalationEntry]:
        return self._entries.get(self._key(alert))

    def increment(self, alert: Alert) -> None:
        entry = self._entries.get(self._key(alert))
        if entry:
            entry.escalation_count += 1


def should_escalate(
    alert: Alert,
    state: EscalationState,
    policy: EscalationPolicy,
    now: Optional[float] = None,
) -> bool:
    """Return True if the alert should be escalated based on elapsed time."""
    if alert.level == AlertLevel.CRITICAL:
        return False
    entry = state.get(alert)
    if entry is None:
        return False
    if entry.escalation_count >= policy.max_escalations:
        return False
    elapsed = (now or time()) - entry.first_seen
    return elapsed >= policy.escalate_after_seconds


def escalate_alert(alert: Alert, policy: EscalationPolicy) -> Alert:
    """Return a copy of the alert with the escalated level."""
    return Alert(
        pipeline=alert.pipeline,
        level=policy.escalate_to,
        metric=alert.metric,
        message=f"[ESCALATED] {alert.message}",
    )
