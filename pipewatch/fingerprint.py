"""Alert fingerprinting — generate stable identifiers for alerts to support deduplication and tracking."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Optional

from pipewatch.alerts import Alert, AlertLevel


@dataclass
class Fingerprint:
    """A stable identifier for an alert based on its key attributes."""

    pipeline: str
    metric: str
    level: str
    digest: str

    def short(self) -> str:
        """Return the first 8 characters of the digest."""
        return self.digest[:8]

    def __str__(self) -> str:
        return f"{self.pipeline}/{self.metric}/{self.level}:{self.short()}"


def _stable_dict(alert: Alert) -> dict:
    """Extract the stable fields from an alert for fingerprinting."""
    return {
        "pipeline": alert.pipeline,
        "metric": alert.metric,
        "level": alert.level.value if isinstance(alert.level, AlertLevel) else str(alert.level),
    }


def compute_fingerprint(alert: Alert) -> Fingerprint:
    """Compute a stable fingerprint for the given alert."""
    data = _stable_dict(alert)
    raw = json.dumps(data, sort_keys=True).encode()
    digest = hashlib.sha256(raw).hexdigest()
    return Fingerprint(
        pipeline=data["pipeline"],
        metric=data["metric"],
        level=data["level"],
        digest=digest,
    )


def fingerprint_alerts(alerts: list[Alert]) -> dict[str, Fingerprint]:
    """Return a mapping of alert index (str) to its fingerprint."""
    return {str(i): compute_fingerprint(a) for i, a in enumerate(alerts)}


def group_by_fingerprint(alerts: list[Alert]) -> dict[str, list[Alert]]:
    """Group alerts by their fingerprint digest."""
    groups: dict[str, list[Alert]] = {}
    for alert in alerts:
        fp = compute_fingerprint(alert)
        groups.setdefault(fp.digest, []).append(alert)
    return groups
