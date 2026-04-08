"""High-level history facade: wraps snapshot + diff for use in watcher/reporter."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric
from pipewatch.snapshot import (
    DEFAULT_SNAPSHOT_DIR,
    load_snapshot,
    save_snapshot,
)
from pipewatch.diff import MetricDiff, compute_diff


@dataclass
class HistoryEntry:
    pipeline_name: str
    current: PipelineMetric
    diff: Optional[MetricDiff] = None


@dataclass
class HistoryReport:
    entries: List[HistoryEntry] = field(default_factory=list)

    def has_regressions(self) -> bool:
        """Return True if any pipeline shows a worsening failure rate."""
        return any(
            e.diff is not None and e.diff.failure_rate_delta > 0
            for e in self.entries
        )

    def status_changes(self) -> List[HistoryEntry]:
        return [e for e in self.entries if e.diff and e.diff.status_changed]


def record_and_diff(
    metrics: List[PipelineMetric],
    snapshot_dir: str = DEFAULT_SNAPSHOT_DIR,
) -> HistoryReport:
    """For each metric: load previous snapshot, compute diff, then save current.

    Returns a HistoryReport with all entries populated.
    """
    entries: List[HistoryEntry] = []
    for metric in metrics:
        previous = load_snapshot(metric.pipeline_name, directory=snapshot_dir)
        diff = compute_diff(metric, previous)
        save_snapshot(metric, directory=snapshot_dir)
        entries.append(HistoryEntry(
            pipeline_name=metric.pipeline_name,
            current=metric,
            diff=diff,
        ))
    return HistoryReport(entries=entries)
