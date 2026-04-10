"""Stale alert detection: flag alerts that have not resolved within a TTL window."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.alerts import Alert, AlertLevel


@dataclass
class StaleAlertConfig:
    ttl_seconds: int = 300  # 5 minutes default
    min_level: AlertLevel = AlertLevel.WARNING


@dataclass
class StaleAlertEntry:
    alert_key: str
    first_seen: datetime
    last_seen: datetime
    count: int = 1

    def age_seconds(self, now: Optional[datetime] = None) -> float:
        now = now or datetime.now(timezone.utc)
        return (now - self.first_seen).total_seconds()

    def is_stale(self, ttl: int, now: Optional[datetime] = None) -> bool:
        return self.age_seconds(now) >= ttl


@dataclass
class StaleAlertState:
    _entries: Dict[str, StaleAlertEntry] = field(default_factory=dict)

    def _key(self, alert: Alert) -> str:
        return f"{alert.pipeline}:{alert.metric}:{alert.level.value}"

    def record(self, alert: Alert, now: Optional[datetime] = None) -> StaleAlertEntry:
        now = now or datetime.now(timezone.utc)
        key = self._key(alert)
        if key in self._entries:
            entry = self._entries[key]
            entry.last_seen = now
            entry.count += 1
        else:
            entry = StaleAlertEntry(alert_key=key, first_seen=now, last_seen=now)
            self._entries[key] = entry
        return entry

    def resolve(self, alert: Alert) -> None:
        self._entries.pop(self._key(alert), None)

    def stale_alerts(
        self,
        alerts: List[Alert],
        config: StaleAlertConfig,
        now: Optional[datetime] = None,
    ) -> List[Alert]:
        now = now or datetime.now(timezone.utc)
        stale: List[Alert] = []
        for alert in alerts:
            if alert.level.value < config.min_level.value:
                continue
            key = self._key(alert)
            entry = self._entries.get(key)
            if entry and entry.is_stale(config.ttl_seconds, now):
                stale.append(alert)
        return stale

    def reset(self) -> None:
        self._entries.clear()
