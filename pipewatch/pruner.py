"""pruner.py — Remove low-signal or redundant alerts based on configurable rules."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.alerts import Alert, AlertLevel


@dataclass
class PrunerConfig:
    """Configuration for the alert pruner."""
    min_level: AlertLevel = AlertLevel.WARNING
    max_alerts: int = 50
    dedupe_pipeline: bool = True
    exclude_pipelines: List[str] = field(default_factory=list)


@dataclass
class PruneResult:
    """Result of a pruning operation."""
    kept: List[Alert]
    removed: List[Alert]

    @property
    def removed_count(self) -> int:
        return len(self.removed)

    @property
    def kept_count(self) -> int:
        return len(self.kept)

    def __str__(self) -> str:
        return f"PruneResult(kept={self.kept_count}, removed={self.removed_count})"


def _below_min_level(alert: Alert, min_level: AlertLevel) -> bool:
    order = [AlertLevel.OK, AlertLevel.WARNING, AlertLevel.CRITICAL]
    return order.index(alert.level) < order.index(min_level)


def prune_alerts(
    alerts: List[Alert],
    config: Optional[PrunerConfig] = None,
) -> PruneResult:
    """Apply pruning rules to a list of alerts and return kept/removed split."""
    if config is None:
        config = PrunerConfig()

    kept: List[Alert] = []
    removed: List[Alert] = []
    seen_pipelines: set = set()

    for alert in alerts:
        # Filter by minimum level
        if _below_min_level(alert, config.min_level):
            removed.append(alert)
            continue

        # Filter excluded pipelines
        if alert.pipeline in config.exclude_pipelines:
            removed.append(alert)
            continue

        # Deduplicate by pipeline (keep first / highest seen)
        if config.dedupe_pipeline and alert.pipeline in seen_pipelines:
            removed.append(alert)
            continue

        seen_pipelines.add(alert.pipeline)
        kept.append(alert)

    # Enforce max_alerts cap
    if len(kept) > config.max_alerts:
        overflow = kept[config.max_alerts:]
        kept = kept[: config.max_alerts]
        removed.extend(overflow)

    return PruneResult(kept=kept, removed=removed)
