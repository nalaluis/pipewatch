"""Mirror module: snapshot and compare pipeline state across two environments."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus


@dataclass
class MirrorEntry:
    pipeline: str
    left: Optional[PipelineMetric]
    right: Optional[PipelineMetric]

    @property
    def only_in_left(self) -> bool:
        return self.left is not None and self.right is None

    @property
    def only_in_right(self) -> bool:
        return self.right is not None and self.left is None

    @property
    def status_diverged(self) -> bool:
        if self.left is None or self.right is None:
            return False
        return self.left.status != self.right.status

    @property
    def failure_rate_delta(self) -> Optional[float]:
        if self.left is None or self.right is None:
            return None
        return self.right.failure_rate - self.left.failure_rate


@dataclass
class MirrorReport:
    left_env: str
    right_env: str
    entries: List[MirrorEntry] = field(default_factory=list)

    @property
    def diverged(self) -> List[MirrorEntry]:
        return [e for e in self.entries if e.status_diverged]

    @property
    def missing_left(self) -> List[MirrorEntry]:
        return [e for e in self.entries if e.only_in_right]

    @property
    def missing_right(self) -> List[MirrorEntry]:
        return [e for e in self.entries if e.only_in_left]


def build_mirror(
    left_env: str,
    right_env: str,
    left_metrics: List[PipelineMetric],
    right_metrics: List[PipelineMetric],
) -> MirrorReport:
    """Compare two sets of pipeline metrics and return a MirrorReport."""
    left_map: Dict[str, PipelineMetric] = {m.pipeline: m for m in left_metrics}
    right_map: Dict[str, PipelineMetric] = {m.pipeline: m for m in right_metrics}
    all_names = sorted(set(left_map) | set(right_map))
    entries = [
        MirrorEntry(
            pipeline=name,
            left=left_map.get(name),
            right=right_map.get(name),
        )
        for name in all_names
    ]
    return MirrorReport(left_env=left_env, right_env=right_env, entries=entries)


def format_mirror(report: MirrorReport) -> str:
    """Render a MirrorReport as a human-readable string."""
    lines = [f"Mirror: {report.left_env} <-> {report.right_env}"]
    lines.append(f"  Diverged:      {len(report.diverged)}")
    lines.append(f"  Only in left:  {len(report.missing_right)}")
    lines.append(f"  Only in right: {len(report.missing_left)}")
    for entry in report.diverged:
        left_s = entry.left.status.value if entry.left else "missing"
        right_s = entry.right.status.value if entry.right else "missing"
        lines.append(f"  [DIVERGED] {entry.pipeline}: {left_s} -> {right_s}")
    for entry in report.missing_right:
        lines.append(f"  [LEFT ONLY] {entry.pipeline}")
    for entry in report.missing_left:
        lines.append(f"  [RIGHT ONLY] {entry.pipeline}")
    return "\n".join(lines)
