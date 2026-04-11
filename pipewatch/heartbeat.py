"""Heartbeat tracking for pipeline liveness monitoring."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class HeartbeatConfig:
    timeout_seconds: float = 60.0
    warning_seconds: float = 30.0


@dataclass
class HeartbeatEntry:
    pipeline: str
    last_seen: float = field(default_factory=time.time)

    def age_seconds(self, now: Optional[float] = None) -> float:
        return (now or time.time()) - self.last_seen

    def is_alive(self, timeout: float, now: Optional[float] = None) -> bool:
        return self.age_seconds(now) < timeout


@dataclass
class MissingHeartbeat:
    pipeline: str
    age_seconds: float
    warning: bool
    critical: bool

    def __str__(self) -> str:
        level = "CRITICAL" if self.critical else "WARNING"
        return f"[{level}] {self.pipeline} last seen {self.age_seconds:.1f}s ago"


class HeartbeatState:
    def __init__(self) -> None:
        self._entries: Dict[str, HeartbeatEntry] = {}

    def record(self, pipeline: str, now: Optional[float] = None) -> None:
        ts = now or time.time()
        self._entries[pipeline] = HeartbeatEntry(pipeline=pipeline, last_seen=ts)

    def last_seen(self, pipeline: str) -> Optional[float]:
        entry = self._entries.get(pipeline)
        return entry.last_seen if entry else None

    def reset(self, pipeline: str) -> None:
        self._entries.pop(pipeline, None)

    def check_missing(
        self,
        pipelines: List[str],
        config: HeartbeatConfig,
        now: Optional[float] = None,
    ) -> List[MissingHeartbeat]:
        now = now or time.time()
        missing: List[MissingHeartbeat] = []
        for name in pipelines:
            entry = self._entries.get(name)
            if entry is None:
                missing.append(
                    MissingHeartbeat(
                        pipeline=name,
                        age_seconds=float("inf"),
                        warning=True,
                        critical=True,
                    )
                )
            else:
                age = entry.age_seconds(now)
                if age >= config.warning_seconds:
                    missing.append(
                        MissingHeartbeat(
                            pipeline=name,
                            age_seconds=age,
                            warning=age >= config.warning_seconds,
                            critical=age >= config.timeout_seconds,
                        )
                    )
        return missing
